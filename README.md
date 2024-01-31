# RabinFP-Dedup

Deduplication Mechanism based on Rabin Fingerprint.
(Exercise from class CSCI4180: Introduction to Cloud Computing and Storage)

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
