import os
import sys
import requests
from typing import Tuple

JAR_URL = "https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/0.14.1/iceberg-spark-runtime-3.3_2.12-0.14.1.jar"

def get_jar_file_path_and_name() -> Tuple[str, str]:
    jar_file_name = JAR_URL.rsplit("/", 1)[-1]
    current_dir = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
    jar_dir = f"{current_dir}/jars"
    return (jar_dir, jar_file_name)

def download_jar(jar_dir, jar_file_name):
    os.makedirs(jar_dir, exist_ok=True)

    with open(os.path.join(jar_dir, jar_file_name), "wb") as jar_file:
        response = requests.get(JAR_URL, stream=True, verify=True)
        response.raise_for_status()
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                jar_file.write(chunk)


if __name__ == "__main__":
    jar_dir, jar_file_name = get_jar_file_path_and_name()
    dest = os.path.join(jar_dir, jar_file_name)
    if os.path.isfile(dest):
        print(f"File already exists: {dest}")
        sys.exit(-1)

    os.makedirs(jar_dir, exist_ok=True)

    download_jar(jar_dir, jar_file_name)
    print(f"Jar downloaded to: {dest}")
