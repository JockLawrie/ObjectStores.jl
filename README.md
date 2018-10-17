# Storage

This repo defines a common API for bucket storage.

This allows changing storage locations without changing your code.

Storage locations include in-memory, local disk and various cloud providers.


# What is bucket storage?


# Permissions

- read only (`:readonly`)
- limited write (`:limited`): Can only update and delete buckets and objects that the storage instance created.
- unlimited write access (`:unlimited`)


# API

Constructor

store = StorageType(mode, root_bucket_name)

Buckets

createbucket!(store, bucketname)
listcontents(store,  bucketname)
deletebucket!(store, bucketname)

Objects

store["mybucket/myobject"] = value     # Create/update. Not possible if the bucket doesn't exist.
store["mybucket/myobject"]             # Read. Returns nothing if the object doesn't exist.
delete!(store, "mybucket/myobject")

Conveniences

isbucket(store,  bucketname)  # True if bucketname refers to a bucket
hasbucket(store, bucketname)  # True if bucketname refers to a bucket in the store

isobject(store,  objectname)  # True if objectname refers to an object
hasobject(store, objectname)  # True if objectname refers to an object in the store

islocal(store)  # Returns true if the storage location is on the same machine as the store instance





This repo contains types and methods for storing data using Julia's key-value API.
It allows applications to configure their storage without changing any code.
All storage types are a subtype of `AbstractStorage`.
Currently only `LocalDisk` is supported.


## LocalDisk

A `LocalDisk` instance enables applications to create, read, update and delete files on the local file system.

For safety, a `LocalDisk` instance cannot delete files/directories that it did not create.

However, an instance can read and update a file it did not create. This allows files to be shared among multiple instances.


```julia
using Storage

storage = LocalDisk("/tmp/testdir/")

storage["mydata"] = "This is my data."  # Write data to  "/tmp/testdir/mydata"
data = String(storage["mydata"])        # Read data from "/tmp/testdir/mydata"

storage["mydata"] = "This new data overwrites my old data. It does not append to my old data."

delete!(storage, "mydata")

storage["dir1/data1"] = "Here is some data."
storage["dir1/data2"] = "Here is some more data."
storage["dir1"]       = "Here comes an error."       # Error: An existing directory cannot be a storage key.
storage["dir2/"]      = "Here comes another error."  # Error: A non-existing directory cannot be a storage key.

delete!(storage, "dir1")                             # Error: Cannot delete non-empty directory.
deleteall!(storage, "dir1")                          # Delete dir1 and all its contents.
delete!(storage, "../")                              # Error: Cannot write to a location that contains ".."
deleteall!(storage)                                  # Equivalent to deleteall!(storage, storage.prefix)

mkdir("/tmp/testdir2")
delete!(storage, "/tmp/testdir2")                    # Error: Cannot delete directory that was not created by storage.
rm("/tmp/testdir2")                                  # Clean up
```
