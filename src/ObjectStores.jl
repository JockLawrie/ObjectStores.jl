module ObjectStores

export ObjectStore, ObjectStoreClient,             # Types
       createbucket!, listcontents, deletebucket!, # Buckets: create/update, read, delete
       setindex!, getindex, delete!,               # Objects: create/update, read, delete
       islocal, isbucket, isobject,                # Conveniences
       Permission,                                 # Re-exported from Authorization
       getpermission, setpermission!, setexpiry!,  # Re-exported from Authorization
       haspermission, permissions_conflict,        # Re-exported from Authorization
       @add_required_fields_storageclient          # Used when constructing concrete subtypes of ObjectStoreClient


using Authorization
using Logging


# Methods that are extended in this package
import Base.setindex!, Base.getindex, Base.delete!
import Authorization.setpermission!


################################################################################
# Types 

abstract type ObjectStoreClient end

macro add_required_fields_storageclient()
    return esc(:(
                 bucket_type::DataType;
                 object_type::DataType;
                ))
end


struct ObjectStore{T <: ObjectStoreClient} <: AbstractClient
    @add_required_fields_client  # From Authorization.jl: id, id2permission, idpattern2permission, type2permission
    rootbucketID::String         # ID of root bucket
    storageclient::T             # Client of back-end storage

    function ObjectStore(id, id2permission, idpattern2permission, type2permission, rootbucketID, storageclient)
        m = parentmodule(typeof(storageclient))
        m._isobject(rootbucketID) && error("Root already exists as an object. Cannot use it as a bucket.")
        newstore = new{typeof(storageclient)}(id, id2permission, idpattern2permission, type2permission, rootbucketID, storageclient)
        if !m._isbucket(rootbucketID)      # Root does not exist...create it
            msg = createbucket!(newstore)  # One arg implies bucketname is root
            msg != nothing && @warn msg    # Couldn't create root bucket...warn
        end
        newstore
    end
end

function ObjectStore(id::String, rootbucketID::String, storageclient)
    id2permission        = Dict{String, Permission}()
    idpattern2permission = Dict{Regex,  Permission}()
    type2permission      = Dict{DataType, Permission}()
    ObjectStore(id, id2permission, idpattern2permission, type2permission, rootbucketID, storageclient)
end

ObjectStore(rootbucketID, storageclient) = ObjectStore("", rootbucketID, storageclient)


################################################################################
# Buckets

"Create bucket. If successful return nothing, else return an error message as a String."
function createbucket!(store::ObjectStore, bucketname::String="")
    if bucketname == ""
        resourceid = store.rootbucketID
    else
        resourceid = normpath(joinpath(store.rootbucketID, bucketname))
        n = length(store.rootbucketID)
        (length(resourceid) < n || resourceid[1:n] != store.rootbucketID) && return "Cannot create a bucket outside the root bucket"
    end
    B = store.storageclient.bucket_type
    create!(store, B(resourceid))
end


"List the contents of the bucket. If successful return the value, else @warn the error message and return nothing."
function listcontents(store::ObjectStore, bucketname::String="")
    if bucketname == ""
        resourceid = store.rootbucketID
    else
        resourceid = normpath(joinpath(store.rootbucketID, bucketname))
        n = length(store.rootbucketID)
        if length(resourceid) < n || resourceid[1:n] != store.rootbucketID
            @warn "Cannot read a bucket outside the root bucket"
            nothing
        end
    end
    B = store.storageclient.bucket_type
    ok, val = read(store, B(resourceid))
    if !ok
        @warn val
        return nothing
    end
    val
end


"Delete bucket. If successful return nothing, else return an error message as a String."
function deletebucket!(store::ObjectStore, bucketname::String="")
    if bucketname == ""
        resourceid = store.rootbucketID
    else
        resourceid = normpath(joinpath(store.rootbucketID, bucketname))
        n = length(store.rootbucketID)
        (length(resourceid) < n || resourceid[1:n] != store.rootbucketID) && return "Cannot delete a bucket outside the root bucket"
    end
    B = store.storageclient.bucket_type
    delete!(store, B(resourceid))
end


################################################################################
# Objects

"Create/update object. If successful return nothing, else return an error message as a String."
function setindex!(store::ObjectStore, v, i::String)
    resourceid = normpath(joinpath(store.rootbucketID, i))
    n = length(store.rootbucketID)
    (length(resourceid) < n || resourceid[1:n] != store.rootbucketID) && return "Cannot create/update an object outside the root bucket"
    O = store.storageclient.object_type
    create!(store, O(resourceid), v)
end


"Read object. If successful return the value, else @warn the error message and return nothing."
function getindex(store::ObjectStore, i::String)
    resourceid = normpath(joinpath(store.rootbucketID, i))
    n = length(store.rootbucketID)
    if length(resourceid) < n || resourceid[1:n] != store.rootbucketID
        @warn "Cannot read an object outside the root bucket"
        return nothing
    end
    O = store.storageclient.object_type
    ok, val = read(store, O(resourceid))
    if !ok
        @warn val
        return nothing
    end
    val
end


"Delete object. If successful return nothing, else return an error message as a String."
function delete!(store::ObjectStore, i::String)
    resourceid = normpath(joinpath(store.rootbucketID, i))
    n = length(store.rootbucketID)
    (length(resourceid) < n || resourceid[1:n] != store.rootbucketID) && return "Cannot delete an object outside the root bucket"
    O = store.storageclient.object_type
    delete!(store, O(resourceid))
end


################################################################################
# Conveniences

"Returns true if the storage backend is on the same machine as the store instance."
function islocal(store::ObjectStore)
    storageclient = store.storageclient
    m = parentmodule(typeof(store.storageclient))
    m._islocal(storageclient)
end

"Returns true if name refers to a bucket."
function isbucket(store::ObjectStore, name::String)
    resourceid = normpath(joinpath(store.rootbucketID, name))
    n = length(store.rootbucketID)
    if length(resourceid) < n || resourceid[1:n] != store.rootbucketID
        @warn "Cannot access buckets or objects outside the root bucket"
        false
    else
        m = parentmodule(typeof(store.storageclient))
        m._isbucket(resourceid)
    end
end

"Returns true if name refers to an object."
function isobject(store::ObjectStore, name::String)
    resourceid = normpath(joinpath(store.rootbucketID, name))
    n = length(store.rootbucketID)
    if length(resourceid) < n || resourceid[1:n] != store.rootbucketID
        @warn "Cannot access buckets or objects outside the root bucket"
        false
    else
        m = parentmodule(typeof(store.storageclient))
        m._isobject(resourceid)
    end
end

function setpermission!(store::ObjectStore, resourcetype::Symbol, p::Permission)
    resourcetype == :bucket && return setpermission!(store, store.storageclient.bucket_type, p)
    resourcetype == :object && return setpermission!(store, store.storageclient.object_type, p)
    @warn "Resource type unknown. Permission not set."
end


end
