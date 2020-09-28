from io import BytesIO
import os

import pytest

import pandas.util._test_decorators as td

from pandas import read_csv
import pandas._testing as tm


def test_streaming_s3_objects():
    # GH17135
    # botocore gained iteration support in 1.10.47, can now be used in read_*
    pytest.importorskip("botocore", minversion="1.10.47")
    from botocore.response import StreamingBody

    data = [b"foo,bar,baz\n1,2,3\n4,5,6\n", b"just,the,header\n"]
    for el in data:
        body = StreamingBody(BytesIO(el), content_length=len(el))
        read_csv(body)


@tm.network
@td.skip_if_no("s3fs")
def test_read_without_creds_from_pub_bucket():
    # GH 34626
    # Use Amazon Open Data Registry - https://registry.opendata.aws/gdelt
    result = read_csv("s3://gdelt-open-data/events/1981.csv", nrows=3)
    assert len(result) == 3


@tm.network
@td.skip_if_no("s3fs")
def test_read_with_creds_from_pub_bucket():
    # Ensure we can read from a public bucket with credentials
    # GH 34626
    # Use Amazon Open Data Registry - https://registry.opendata.aws/gdelt

    with tm.ensure_safe_environment_variables():
        # temporary workaround as moto fails for botocore >= 1.11 otherwise,
        # see https://github.com/spulec/moto/issues/1924 & 1952
        os.environ.setdefault("AWS_ACCESS_KEY_ID", "foobar_key")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "foobar_secret")
        df = read_csv(
            "s3://gdelt-open-data/events/1981.csv", nrows=5, sep="\t", header=None
        )
        assert len(df) == 5

@td.skip_if_no("s3fs")
def test_read_changing_s3_object(s3_resource, s3_base, s3so):
    # Ensure that reading an object from s3 works before and 
    # after writing more data to the object
    df1 = read_csv("s3://pandas-test/changing_file.csv", storage_options=s3so)

    # duplicate the contents of the dataframe, and 
    # write the larger dataframe into the same file
    df2 = df1.append(df1)

    import boto3
    conn = boto3.resource("s3", endpoint_url=s3_base)
    cli = boto3.client("s3", endpoint_url=s3_base)
    cli.put_object(
        Bucket="pandas-test", 
        Key="changing_file.csv", 
        Body=df2.to_csv(path_or_buf=None)
        )
    
    # read the file with more data
    df3 = read_csv("s3://pandas-test/changing_file.csv", index_col=0, storage_options=s3so)
    tm.assert_equal(df2, df3)
