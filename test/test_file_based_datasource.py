import logging
import os
from typing import Iterator

import pyarrow
import ray
import ray.data.block
import ray.data.datasource.file_based_datasource
import ray.data.extensions

CRAZY_HIGH_NUMBER = 999999999999999999


class CustomDatasource(ray.data.datasource.file_based_datasource.FileBasedDatasource):
    def _read_stream(
        self, f: "pyarrow.NativeFile", path: str, **reader_args
    ) -> Iterator[ray.data.block.Block]:
        logger = logging.getLogger(__name__)

        logger.debug("Start reading from path %s", path)
        with open(path) as f:
            for i_line, line in enumerate(f.readlines()):
                logger.debug("  line %s = %s", i_line, line)
                yield pyarrow.Table.from_pydict({"value": line})


def main():
    ray.init(
        include_dashboard=False,
        local_mode=True,
    )

    path = os.path.join(
        # path to the folder of this script
        os.path.dirname(os.path.abspath(__file__)),
        "data",
    )
    paths = [path]

    ds = ray.data.read_datasource(
        CustomDatasource(),
        paths=paths,
        # crazy high number triggers an issue
        parallelism=CRAZY_HIGH_NUMBER,
    )
    some_batch = next(ds.iter_batches(batch_size=1))
    print(some_batch)


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger(__name__).setLevel(logging.DEBUG)

    main()
