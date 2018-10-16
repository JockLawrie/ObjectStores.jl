module BucketStores


export BucketStore, AbstractBackend,                      # Types
       createbucket!, listbucket, deletebucket!,          # Buckets
       setindex!, getindex, delete!,                      # Objects
       isbucket, hasbucket, isobject, hasobject, islocal, # Conveniences
       LocalDisk  # temporary


import Base.setindex!, Base.getindex, Base.delete!


abstract type AbstractBackend end


################################################################################
# Constructors

struct BucketStore{T<:AbstractBackend}
    permission::Symbol                # One of :readonly, :limited, :unlimited
    root::String                      # Name of root bucket, which contains all buckets and objects in the store.
    names::Dict{String, Set{String}}  # bucketname => Set(bucket_names..., object_names...). bucketname excludes root. Names exclude root/bucketname.
    bucketnames::Set{String}          # Names of buckets created by the store (not all buckets in names were created by the store).
    backend::T

    function BucketStore(permission, root, names, bucketnames, backend)
        !in(permission, Set([:readonly, :limited, :unlimited])) && error("Permission must be one of :readonly, :limited, :unlimited")
        new(permission, root, names, bucketnames, backend)
    end
end

BucketStore(permission, root, backend) = BucketStore(permission, root, Dict{String, Set{String}}(), Set{String}(), backend)


################################################################################
# API: Buckets

"""
Modified: store.names

Return true if all checks pass, else return false.
"""
function createbucket!(store::BucketStore{<:AbstractBackend}, bktname::String)
    result = false
    if store.permission != :readonly  # Store has write permission
        result = createbucket!(store.backend, fullpath)
        if result == true
            push!(store.bucketnames, bktname)
            store.names[bktname] = Set{String}()
            haskey(store.names, cb) && push!(store.names[cb], shortname)
        end
    end
    result
end


"""
Returns a list (Vector) of buckets and objects contained in the given bucket if it exists, returns nothing otherwise.

The list includes buckets and objects not created by the BucketStore instance.
"""
listbucket(store::BucketStore{<:AbstractBackend}, bktname::String) = listbucket(store.backend, joinpath(store.root, bktname))


"""
Modified: store.names

Return true if all checks pass, else return false.
"""
function deletebucket!(store::BucketStore{<:AbstractBackend}, bktname::String)
    result = false
    if (store.permission == :limited && hasbucket(store, bktname)) || store.permission == :unlimited
        result = deletebucket!(store.backend, joinpath(store.root, bktname))
        if result == true
            hasbucket(store, bktname)    && pop!(store.bucketnames, bktname)
            haskey(store.names, bktname) && delete!(store.names, bktname)
            cb, shortname = splitdir(bktname)
            haskey(store.names, cb) && pop!(store.names[cb], shortname)
        end
    end
    result
end


################################################################################
# API: Objects

function setindex!(store::BucketStore{<:AbstractBackend}, v, i::String)
    store.permission == :readonly && return false  # Store does not have permission to create/update objects

    # Run checks
    permission = store.permission
    fullpath   = joinpath(store.root, i)
    if permission == :limited && !hasobject(store, i) && isobject(store.backend, fullpath) # Object pre-exists and is not in the store...cannot modify it
        return false
    end

    # Execute
    result = setindex!(store.backend, v, fullpath)
    if result == true
        cb, shortname = splitdir(i)
        haskey(store.names, cb) && push!(store.names[cb], shortname)
    end
    result
end


"Returns the object if it exists, returns nothing otherwise."
getindex(store::BucketStore{<:AbstractBackend}, i::String) = getindex(store.backend, joinpath(store.root, i))


function delete!(store::BucketStore{<:AbstractBackend}, i::String)
    result = false
    if (store.permission == :limited && hasobject(store, i)) || store.permission == :unlimited
        result = delete!(store.backend, joinpath(store.root, i))
        if result == true
            cb, shortname = splitdir(i)
            haskey(store.names, cb) && pop!(store.names[cb], shortname)
        end
    end
    result
end


################################################################################
# API: Conveniences

"Returns true if name refers to a bucket."
isbucket(store::BucketStore{<:AbstractBackend}, name::String) = isbucket(store.backend, joinpath(store.root, name))

"Returns true if the bucket is in the store."
hasbucket(store::BucketStore{<:AbstractBackend}, bktname::String) = in(bktname, store.bucketnames)

"Returns true if name refers to an object."
isobject(store::BucketStore{<:AbstractBackend}, name::String) = isobject(store.backend, joinpath(store.root, name))

"Returns true if the bucket is in the store."
function hasobject(store::BucketStore{<:AbstractBackend}, objectname::String)
    cb, shortname = splitdir(objectname)
    haskey(store.names, cb) && in(shortname, store.names[cb])
end

"Returns true if the storage backend is on the same machine as the store instance."
islocal(store::BucketStore{<:AbstractBackend}) = islocal(store.backend)

end
