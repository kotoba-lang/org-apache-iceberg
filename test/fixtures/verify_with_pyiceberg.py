"""Verify that a table THIS REPO WRITES is readable by the reference implementation.

The only thing that makes an Iceberg writer correct is that an Iceberg reader
accepts it. Round-tripping through this repo's own code would prove the writer
and reader share an interpretation, which two halves of one misunderstanding
also do -- and Iceberg is a format where that failure is quiet, because a
manifest with the wrong field ids still parses as Avro and reads back as a
table with no files in it.

    python3 -m venv .venv && .venv/bin/pip install "pyiceberg[pyarrow]"
    .venv/bin/python test/fixtures/verify_with_pyiceberg.py

The data file is written by pyarrow, on purpose: this is a test of the table
format layer -- the manifests and metadata this repo produces -- and using our
own Parquet writer here would fold two independent claims into one result.

Exits non-zero on the first disagreement.
"""
import json
import pathlib
import subprocess
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.table import StaticTable

REPO = pathlib.Path(__file__).resolve().parents[2]

ROWS = {"id": ["a", "b", "c"], "n": [1, 2, 3]}


def clojure_writes_table(location: pathlib.Path, data_file: pathlib.Path,
                         rows: int) -> pathlib.Path:
    """Drive iceberg.table/append and write every file it returns."""
    metadata_path = location / "metadata" / "v1.metadata.json"
    program = f"""
    (require '[iceberg.table :as t] '[iceberg.schema :as s] '[clojure.java.io :as io])
    (defn spit-bytes [p bs]
      (io/make-parents p)
      (with-open [o (io/output-stream p)]
        (.write o (byte-array (map unchecked-byte bs)))))
    (let [schema (s/schema [(s/field 1 "id" :string) (s/field 2 "n" :long)])
          out (t/append {{:location "{location}"
                          :schema schema
                          :data-files [{{:file-path "{data_file}"
                                         :record-count {rows}
                                         :file-size-bytes {data_file.stat().st_size}}}]}})]
      (doseq [[p bs] (:files out)] (spit-bytes p bs))
      (io/make-parents "{metadata_path}")
      (spit "{metadata_path}" (:metadata-json out)))
    """
    subprocess.run(["clojure", "-M", "-e", program], cwd=REPO, check=True,
                   stdout=subprocess.DEVNULL)
    return metadata_path


def main() -> int:
    failures = 0
    with tempfile.TemporaryDirectory() as tmp:
        location = pathlib.Path(tmp) / "warehouse" / "t"
        data_dir = location / "data"
        data_dir.mkdir(parents=True)
        data_file = data_dir / "part-0.parquet"
        table = pa.table(ROWS)
        pq.write_table(table, data_file)

        metadata_path = clojure_writes_table(location, data_file, table.num_rows)

        try:
            t = StaticTable.from_metadata(str(metadata_path))
        except Exception as exc:
            print(f"FAIL: pyiceberg refused the metadata: {type(exc).__name__}: {exc}")
            return 1

        print(f"ok   schema      {t.schema()}")
        snap = t.current_snapshot()
        print(f"ok   snapshot    id={snap.snapshot_id} seq={snap.sequence_number} "
              f"op={snap.summary.operation.value}")

        files = list(t.scan().plan_files())
        if len(files) != 1:
            print(f"FAIL: expected 1 data file in the scan, got {len(files)}")
            failures += 1
        else:
            print(f"ok   plan_files  {len(files)} file, "
                  f"{files[0].file.record_count} records")

        try:
            got = t.scan().to_arrow().to_pydict()
        except Exception as exc:
            print(f"FAIL: scan could not read the data: {type(exc).__name__}: {exc}")
            return 1

        if got != ROWS:
            print(f"FAIL: rows differ\n  got  {got}\n  want {ROWS}")
            failures += 1
        else:
            print(f"ok   scan        {len(got['id'])} rows, values equal")

    print("VERIFIED against pyiceberg" if not failures else f"{failures} FAILURES")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
