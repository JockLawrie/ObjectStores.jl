# ObjectStores

This package defines a client for object storage that allows changing storage back-ends without changing your code.

Storage back-ends include in-memory, local disk and cloud-based object storage.


# What is an object store?

In an object store, data is stored as objects and objects are grouped into buckets.

Examples of storage back-ends include:
- [`LocalDiskObjectStores.jl`](), which uses the local file system to store objects (files) in buckets (directories).
- [`GCSObjectStores.jl`](), which uses Google Cloud Storage.


# Permissions

An object store's access to buckets and objects is controlled using the small but flexible [`Authorization.jl`]() framework.
This framework allows both fine-grained access control for specific buckets and objects, as well as more coarse access control such as uniform access for all buckets and/or objects. Please read the [`README`]() to understand how to access buckets and objects can be controlled.

_Note:_ A store cannot act on (create/read/delete) buckets or objects that are outside the root bucket.

# Example Usage

See the examples and tests in [`LocalDiskObjectStores.jl`](), which uses the local file system as the storage back-end.


# API

```julia
#=
  Constructor:
    store = ObjectStore(rootbucketID::String, storageclient::T) where {T <: ObjectStorageClient}

  Examples:
    store = ObjectStore("/tmp/rootbucket", LocalDiskObjectStores.Client())     # Objects/buckets are on the local file system
    store = ObjectStore("myrootbucket", GCSObjectStores.Client("creds.json"))  # Objects/buckets are on Google Cloud Storage

  The code below will work regardless of which storage back-end is used (using either constructor above).
=#


# Set access permissions
setpermission!(store, :bucket, Permission(false, true, false, false))  # Bucket access is cRud (read-only) without expiry
setpermission!(store, :object, Permission(false, true, false, false))  # Object access is cRud (read-only) without expiry

# Allow CRUD (read/write) access for 5 minutes to objects in the bucket called "mybucket"
setpermission!(store, r"rootbucket/mybucket/*", Permission(true, true, true, true, now() + Minute(5)))

# Buckets

createbucket!(store, "mybucket")  # Create mybucket in the root bucket
listcontents(store,  "mybucket")  # List the contents of rootbucket/mybucket. Return nothing if it doesn't exist
deletebucket!(store, "mybucket")  # Delete rootbucket/mybucket if it exists

# Objects

store["mybucket/myobject"] = value     # Create/update. Not possible if the bucket doesn't exist.
store["mybucket/myobject"]             # Read. Returns nothing if the object doesn't exist.
delete!(store, "mybucket/myobject")

# Conveniences

isbucket(store,  bucketname)  # True if bucketname refers to a bucket
isobject(store,  objectname)  # True if objectname refers to an object
islocal(store)                # Returns true if the storage location is on the same machine as the store instance

# Permission queries
p = getpermission(store, bucket_or_object)  # Get the permission settings for the specific bucket/object
setexpiry!(store, now() + Hour(1))          # Set an expiry for all the buckets/objects that the store has access to

haspermission(store, bucket_or_object, :create)  # True if the store has :create access to the bucket/object
permissions_conflict(store, "mybucket")          # True if the rules that define the store's access to "mybucket" conflict
```
