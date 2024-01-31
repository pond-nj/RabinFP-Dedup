# RabinFP-Dedup

Deduplication Mechanism based on Rabin Fingerprint

To upload a file
```
./upload.sh $filename
```

To download a file, this will download the file to filename `d-$filename`
```
./download.sh $filename
```

To run test (uplaod, download, and then diff the file)
```
./run_test.sh $filename
```
