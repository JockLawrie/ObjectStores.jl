module AbstractBucketStores

export BucketStore, AbstractStorageBackend,        # Types
       listcontents, createbucket!, deletebucket!, # Buckets: read, create/update, delete
       getindex, setindex!, delete!,               # Objects: read, create/update, delete
       islocal, isbucket, isobject                 # Conveniences

using Authorization

import Base.setindex!, Base.getindex, Base.delete!


################################################################################
# Types 

abstract type AbstractStorageBackend end  # Required fields: bucket_type, object_type

struct BucketStore{T <: AbstractStorageBackend} <: AbstractClient
    root::String  # ID of root bucket
    backend::T
    @add_required_fields_client  # From Authorization.jl: id, id2permission, idpattern2permission, type2permission

    function BucketStore(id, root, backend, id2permission, idpattern2permission, type2permission)
        m = parentmodule(typeof(backend))
        m._isobject(backend, root) && error("Root already exists as an object. Cannot use it as a bucket.")
        newstore = new{typeof(backend)}(root, backend, id, id2permission, idpattern2permission, type2permission)
        if !m._isbucket(backend, root)  # Root does not exist...create it
            ok, msg = createbucket!(newstore, "")  # 2nd arg "" implies bucketname is root
            !ok && error(msg)
        end
        newstore
    end
end


################################################################################
# Buckets

"""
Returns a list (Vector) of the names of the buckets and objects contained in the given bucket if it exists, returns nothing otherwise.

The list includes buckets and objects not created by the BucketStore instance.

If a bucket name is not supplied, the contents of the root bucket are given.
"""
function listcontents(store::BucketStore, bucketname::String="")
    backend = store.backend
    B = backend.bucket_type
    m = parentmodule(typeof(backend))
    resourceid = bucketname == "" ? store.root : joinpath(store.root, bucketname)
    read(store, m.B(resourceid))
end


"""
Modified: store.names

Return true if all checks pass, else return false.

If bucketisroot then create the root bucket.
"""
function createbucket!(store::BucketStore, bucketname::String="")
    backend = store.backend
    B = backend.bucket_type
    m = parentmodule(typeof(backend))
    resourceid = bucketname == "" ? store.root : joinpath(store.root, bucketname)
    create!(store, m.B(resourceid))
end


"""
Modified: store.names

Return true if all checks pass, else return false.

If bucketisroot then delete the root bucket.
"""
function deletebucket!(store::BucketStore, bucketname::String)
    backend = store.backend
    B = backend.bucket_type
    m = parentmodule(typeof(backend))
    resourceid = bucketname == "" ? store.root : joinpath(store.root, bucketname)
    delete!(store, m.B(resourceid))
end


################################################################################
# Objects

"Returns the object if it exists, returns nothing otherwise."
function getindex(store::BucketStore, i::String)
    backend = store.backend
    O = backend.object_type
    m = parentmodule(typeof(backend))
    read(store, m.O(joinpath(store.root, i)))
end


function setindex!(store::BucketStore, v, i::String)
    fullpath = joinpath(store.root, i)
    backend  = store.backend
    O = backend.object_type
    m = parentmodule(typeof(backend))
    m._isbucket(store, fullpath)  && return (false, "$(i) is a bucket, not an object")
    cb, shortname = splitdir(fullpath)
    !m._isbucket(store, cb) && return (false, "Cannot create object $(i) inside a non-existent bucket.")
    create!(store, m.O(fullpath), v)
end


function delete!(store::BucketStore, i::String)
    backend = store.backend
    O = backend.object_type
    m = parentmodule(typeof(backend))
    delete!(store, m.O(joinpath(store.root, i)))
end


################################################################################
# Conveniences

"Returns true if the storage backend is on the same machine as the store instance."
function islocal(store::BucketStore)
    m = parentmodule(typeof(store.backend))
    m._islocal(store)
end

"Returns true if name refers to a bucket."
function isbucket(store::BucketStore, name::String)
    m = parentmodule(typeof(store.backend))
    m._isbucket(store, joinpath(store.root, name))
end

"Returns true if name refers to an object."
function isobject(store::BucketStore, name::String)
    m = parentmodule(typeof(store.backend))
    m._isobject(store, joinpath(store.root, name))
end

end
