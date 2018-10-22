module AbstractBucketStores

export BucketStore, AbstractStorageBackend,        # Types
       createbucket!, listcontents, deletebucket!, # Buckets: create/update, read, delete
       setindex!, getindex, delete!,               # Objects: create/update, read, delete
       islocal, isbucket, isobject                 # Conveniences

using Authorization
using Logging

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
        m._isobject(root) && error("Root already exists as an object. Cannot use it as a bucket.")
        newstore = new{typeof(backend)}(root, backend, id, id2permission, idpattern2permission, type2permission)
        if !m._isbucket(root)  # Root does not exist...create it
            ok, msg = createbucket!(newstore, "")  # 2nd arg "" implies bucketname is root
            !ok && error(msg)
        end
        newstore
    end
end


################################################################################
# Buckets

"Create bucket. If successful return nothing, else return an error message as a String."
function createbucket!(store::BucketStore, bucketname::String="")
    backend = store.backend
    B = backend.bucket_type
    m = parentmodule(typeof(backend))
    resourceid = bucketname == "" ? store.root : joinpath(store.root, bucketname)
    create!(store, m.B(resourceid))
end


"Read bucket. If successful return the value, else @warn the error message and return nothing."
function listcontents(store::BucketStore, bucketname::String="")
    backend = store.backend
    B = backend.bucket_type
    m = parentmodule(typeof(backend))
    resourceid = bucketname == "" ? store.root : joinpath(store.root, bucketname)
    ok, val = read(store, m.B(resourceid))
    if !ok
        @warn val
        return nothing
    end
    val
end


"Delete bucket. If successful return nothing, else return an error message as a String."
function deletebucket!(store::BucketStore, bucketname::String)
    backend = store.backend
    B = backend.bucket_type
    m = parentmodule(typeof(backend))
    resourceid = bucketname == "" ? store.root : joinpath(store.root, bucketname)
    delete!(store, m.B(resourceid))
end


################################################################################
# Objects

"Create/update object. If successful return nothing, else return an error message as a String."
function setindex!(store::BucketStore, v, i::String)
    fullpath = joinpath(store.root, i)
    backend  = store.backend
    m = parentmodule(typeof(backend))
    m._isbucket(fullpath)  && return (false, "$(i) is a bucket, not an object")
    cb, shortname = splitdir(fullpath)
    !m._isbucket(cb) && return (false, "Cannot create object $(i) inside a non-existent bucket.")
    O = backend.object_type
    create!(store, m.O(fullpath), v)
end


"Read object. If successful return the value, else @warn the error message and return nothing."
function getindex(store::BucketStore, i::String)
    backend = store.backend
    O = backend.object_type
    m = parentmodule(typeof(backend))
    ok, val = read(store, m.O(joinpath(store.root, i)))
    if !ok
        @warn val
        return nothing
    end
    val
end


"Delete object. If successful return nothing, else return an error message as a String."
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
    m._islocal(store.backend)
end

"Returns true if name refers to a bucket."
function isbucket(store::BucketStore, name::String)
    m = parentmodule(typeof(store.backend))
    m._isbucket(joinpath(store.root, name))
end

"Returns true if name refers to an object."
function isobject(store::BucketStore, name::String)
    m = parentmodule(typeof(store.backend))
    m._isobject(joinpath(store.root, name))
end

end
