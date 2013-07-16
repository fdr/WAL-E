import boto

from boto import s3
from boto.s3 import connection


_S3_REGIONS = {
    # A map like this is actually defined in boto.s3 in newer versions of boto
    # but we reproduce it here for the folks (notably, Ubuntu 12.04) on older
    # versions.
    'ap-northeast-1': 's3-ap-northeast-1.amazonaws.com',
    'ap-southeast-1': 's3-ap-southeast-1.amazonaws.com',
    'ap-southeast-2': 's3-ap-southeast-2.amazonaws.com',
    'eu-west-1': 's3-eu-west-1.amazonaws.com',
    'us-standard': 's3.amazonaws.com',
    'us-west-1': 's3-us-west-1.amazonaws.com',
    'us-west-2': 's3-us-west-2.amazonaws.com',
}

try:
    # Override the hard-coded region map with boto's mappings if
    # available.
    from boto.s3 import regions
    _S3_REGIONS.update(dict((r.name, r.endpoint) for r in regions()))
except ImportError:
    pass


def _is_ipv4_like(s):
    parts = s.split('.')

    if len(parts) != 4:
        return False

    for part in parts:
        try:
            number = int(part)
        except ValueError:
            return False

        if number < 0 or number > 255:
            return False

    return True


def _is_mostly_subdomain_compatible(bucket_name):
    """Returns True if SubdomainCallingFormat can be used...mostly

    This checks to make sure that putting aside certificate validation
    issues that a bucket_name is able to use the
    SubdomainCallingFormat.
    """
    return (bucket_name.lower() == bucket_name and
            len(bucket_name) >= 3 and
            len(bucket_name) <= 63 and
            '_' not in bucket_name and
            '..' not in bucket_name and
            '-.' not in bucket_name and
            '.-' not in bucket_name and
            not bucket_name.startswith('-') and
            not bucket_name.endswith('-') and
            not bucket_name.startswith('.') and
            not bucket_name.endswith('.') and
            not _is_ipv4_like(bucket_name))


def _connect_secureish(*args, **kwargs):
    """Connect using the safest available options.

    This turns on encryption (for all supported boto versions) and
    certificate validation (in versions of boto where it works) *when
    possible*.

    Versions below 2.6 don't support the validate_certs option to
    S3Connection, and enable it via configuration option just seems to
    cause an error.
    """
    if boto.__version__ >= '2.6.0':
        kwargs['validate_certs'] = True

    kwargs['is_secure'] = True

    return connection.S3Connection(*args, **kwargs)


class CallingInfo(object):
    def __init__(self, bucket_name=None, calling_format=None, region=None,
                 ordinary_endpoint=None):
        self.bucket_name = bucket_name
        self.calling_format = calling_format
        self.region = region
        self.ordinary_endpoint = ordinary_endpoint

    def __repr__(self):
        return ('CallingInfo({bucket_name}, {calling_format!r}, {region!r}, '
                '{ordinary_endpoint!r})'.format(**self.__dict__))

    def __str__(self):
        return repr(self)

    def connect(self, aws_access_key_id, aws_secret_access_key):
        def _conn_help(*args, **kwargs):
            return _connect_secureish(
                 *args,
                 aws_access_key_id=aws_access_key_id,
                 aws_secret_access_key=aws_secret_access_key,
                 calling_format=self.calling_format(),
                 **kwargs)

        # Check if subdomain format compatible; no need to go through
        # nay region detection mumbo-jumbo of any kind.
        if self.calling_format is connection.SubdomainCallingFormat:
            return _conn_help()

        # Check if OrdinaryCallingFormat compatible, but also see if
        # the endpoint has already been set, in which case only
        # setting the host= flag is necessary.
        assert self.calling_format is connection.OrdinaryCallingFormat
        if self.ordinary_endpoint is not None:
            return _conn_help(host=self.ordinary_endpoint)

        # By this point, this is an OrdinaryCallingFormat bucket that
        # has never had its region detected in this CallingInfo
        # instance.  So, detect its region (this can happen without
        # knowing the right regional endpoint) and store it to speed
        # future calls.
        assert self.calling_format is connection.OrdinaryCallingFormat
        assert self.region is None
        assert self.ordinary_endpoint is None

        conn = _conn_help()

        bucket = s3.bucket.Bucket(connection=conn,
                                  name=self.bucket_name)

        try:
            loc = bucket.get_location()
        except boto.exception.S3ResponseError, e:
            if e.status == 403:
                # A 403 can be caused by IAM keys that do not permit
                # GetBucketLocation.  To not change behavior for
                # environments that do not have GetBucketLocation
                # allowed, fall back to the default endpoint,
                # preserving behavior for those using us-standard.
                self.region = 'us-standard'
                self.ordinary_endpoint = _S3_REGIONS[self.region]
            else:
                raise
        else:
            self.region = loc
            self.ordinary_endpoint = _S3_REGIONS[loc]

        # Recurse, now that the region information is filled; this
        # should return a bona-fide usable connection.
        assert self.ordinary_endpoint is not None
        return self.connect(aws_access_key_id, aws_secret_access_key)


def from_bucket_name(bucket_name):
    mostly_ok = _is_mostly_subdomain_compatible(bucket_name)

    if not mostly_ok:
        return CallingInfo(
            bucket_name=bucket_name,
            region='us-standard',
            calling_format=connection.OrdinaryCallingFormat,
            ordinary_endpoint=_S3_REGIONS['us-standard'])
    else:
        if '.' in bucket_name:
            # The bucket_name might have been DNS compatible, but once
            # dots are involved TLS certificate validations will
            # certainly fail even if that's the case.
            #
            # Leave it to the caller to perform the API call, as to
            # avoid teaching this part of the code about credentials.
            return CallingInfo(
                bucket_name=bucket_name,
                calling_format=connection.OrdinaryCallingFormat,
                region=None,
                ordinary_endpoint=None)
        else:
            # SubdomainCallingFormat can be used, with TLS,
            # world-wide, and WAL-E can be region-oblivious.
            #
            # This is because there are no dots in the bucket name,
            # and no other bucket naming abnormalities either.
            return CallingInfo(
                bucket_name=bucket_name,
                calling_format=connection.SubdomainCallingFormat,
                region=None,
                ordinary_endpoint=None)

    assert False
