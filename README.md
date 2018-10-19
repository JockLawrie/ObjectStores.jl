# AbstractBucketStores

This repo defines a common API for bucket storage.

This allows changing storage back-ends without changing your code.

Storage back-ends include in-memory, local disk and bucket storage in the cloud.


# What is bucket storage?

- Store data as objects
- Groups objects into buckets

For example, if the storage back-end is the local file system then objects are files and buckets are directories.


# Permissions

When constructing a store, you must select a permission level for the store.
This determines what read/write privileges the store has.
Currently the permission level must be one of the following:

- read only (`:readonly`)
- limited write (`:limited`): Can only update and delete buckets and objects that the store instance created.
- unlimited write access (`:unlimited`)


# Example Usage

See `LocalDiskStorage.jl`, which implements the bucket store API using the file system as the back-end.


# API

Constructor

```julia
store = StoreType(permission::Symbol, root_bucket_name::String, type_specific_args...)
```

Buckets

```julia
createbucket!(store, bucketname)
listcontents(store,  bucketname)
deletebucket!(store, bucketname)
```

Objects

```julia
store["mybucket/myobject"] = value     # Create/update. Not possible if the bucket doesn't exist.
store["mybucket/myobject"]             # Read. Returns nothing if the object doesn't exist.
delete!(store, "mybucket/myobject")
```

Conveniences

```julia
isbucket(store,  bucketname)  # True if bucketname refers to a bucket
hasbucket(store, bucketname)  # True if bucketname refers to a bucket in the store

isobject(store,  objectname)  # True if objectname refers to an object
hasobject(store, objectname)  # True if objectname refers to an object in the store

islocal(store)  # Returns true if the storage location is on the same machine as the store instance
```
