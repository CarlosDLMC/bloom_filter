import threading
import time
import boto3
import jpype
from jpype import JArray, JByte
import re
from envy import Envy


class NavigationCheck():

    def _s3_get_newest(self, bucket_name, prefix):
        """
        Download the newest object from an S3 bucket that shares a common key prefix.

        Parameters:
        - bucket_name (str): Name of the S3 bucket.
        - prefix (str): Key prefix to filter the objects.
        - download_path (str): Local path to which the object will be downloaded.

        Returns:
        - str: Key of the downloaded object, or None if no matching object was found.
        """

        # List objects in the bucket with the given prefix
        objects = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        # Sort objects by LastModified in descending order
        sorted_objects = sorted(
            objects.get('Contents', []),
            key=lambda x: x['LastModified'],
            reverse=True
        )

        # If we have any objects, download the newest one
        if sorted_objects:
            newest_object_key = sorted_objects[0]['Key']
            print(f">>> Loading key {newest_object_key} from s3")
            obj = self.s3.get_object(Bucket=bucket_name, Key=newest_object_key)
            cnt = obj["Body"].read()
            return cnt
        return None

    def _to_filestream(self, obj):

        dummy = JArray(JByte)(obj)
        dummy = self.ByteArrayInputStream(dummy)

        return dummy

    def _bloom_refresher(self):
        while True:
            with self.lock:
                try:
                    obj = self._s3_get_newest(
                        self.envy.get_env("bloom_s3bucket", "navcheck"),
                        self.envy.get_env("bloom_s3keyprefix", "navcheck") + "/"
                    )
                    stream = self._to_filestream(obj)
                    res = self.BloomFilter.readFrom(stream)

                    print("*" * 50)
                    print(f"***  Phisherman-Lookout")
                    print(f"***  Bloom filter loaded from disk:")
                    print(f"***      Bitsize: {str(res.bitSize())}")
                    print(f"***      Expected FPP: {str(res.expectedFpp())}")
                    print("*" * 50)

                    stream.close()
                    self.bloom = res

                except Exception:
                    pass

            time.sleep(self.bloom_poll_interval)

    def __init__(self):

        self.envy = Envy.find_envy() or Envy()

        self.lock = threading.Lock()
        self.s3 = boto3.client(
            service_name='s3',
            aws_access_key_id=self.envy.get_env("access_key_id", "s3"),
            aws_secret_access_key=self.envy.get_env("secret_access_key", "s3"),
            endpoint_url=self.envy.get_env("endpoint_url", "s3")
        )

        jpype.startJVM(
            classpath=[self.envy.get_env("sketch_jar_path", "navcheck")]
        )

        self.BloomFilter = jpype.JClass("org.apache.spark.util.sketch.BloomFilter")
        self.ByteArrayInputStream = jpype.JClass("java.io.ByteArrayInputStream")
        self.domrex = re.compile(r"""http[s]?:\/\/([^\/\"\:]+).*""")
        self.bloom = None
        self.bloom_poll_interval = int(self.envy.get_env("bloom_refresh_secs", "navcheck"))
        assert self.bloom_poll_interval >= 600

        self.thread = threading.Thread(target=self._bloom_refresher)
        self.thread.start()

    def hostname_is_known(self, url):
        res = {}
        m = re.search(self.domrex, url.lower())
        if m:
            res["hostname"] = m.groups()[0]
            with self.lock:
                res["known_hostname"] = 1 if self.bloom.mightContain(res["hostname"]) else 0

        else:
            res["hostname"] = None
            res["known_hostname"] = -1

        return res
