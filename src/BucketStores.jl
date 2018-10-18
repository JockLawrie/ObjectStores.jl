module BucketStores


export BucketStore, AbstractBackend,                      # Types
       listcontents, createbucket!, deletebucket!,        # Buckets
       getindex, setindex!, delete!,                      # Objects
       islocal, isbucket, hasbucket, isobject, hasobject  # Conveniences


import Base.setindex!, Base.getindex, Base.delete!

abstract type AbstractBackend end

################################################################################
# Constructors

struct BucketStore{T <: AbstractBackend}
    permission::Symbol                # One of :readonly, :limited, :unlimited
    root::String                      # Name of root bucket, which contains all buckets and objects in the store.
    names::Dict{String, Set{String}}  # bucketname => Set(bucket_names..., object_names...). bucketname excludes root. Names exclude root/bucketname.
    bucketnames::Set{String}          # Names of buckets created by the store (not all buckets in names were created by the store).
    backend::T

    function BucketStore(permission, root, backend)
        !in(permission, Set([:readonly, :limited, :unlimited])) && error("Permission must be one of :readonly, :limited, :unlimited")
        isobject(backend, root) && error("Root already exists as an object.")
        newstore = new{typeof(backend)}(permission, root, Dict{String, Set{String}}(root => Set{String}()), Set{String}(), backend)
        if !isbucket(backend, root)
            ok = createbucket!(newstore, root)
            if !ok
                error("Root bucket does not exist and could not be created.")
            end
        end
        newstore
    end
end


################################################################################
# API: Buckets

"""
Returns a list (Vector) of the names of the buckets and objects contained in the given bucket if it exists, returns nothing otherwise.

The list includes buckets and objects not created by the BucketStore instance.

If a bucket name is not supplied, the contents of the root bucket are given.
"""
listcontents(store::BucketStore{<:AbstractBackend}, bktname::String) = listcontents(store.backend, joinpath(store.root, bktname))

listcontents(store::BucketStore{<:AbstractBackend}) = listcontents(store, store.root)


"""
Modified: store.names

Return true if all checks pass, else return false.
"""
function createbucket!(store::BucketStore{<:AbstractBackend}, bktname::String)
    result = false
    if store.permission != :readonly  # Store has write permission
        fullpath = joinpath(store.root, bktname)
        result   = createbucket!(store.backend, fullpath)
        if result == true
            push!(store.bucketnames, bktname)
            store.names[bktname] = Set{String}()
            haskey(store.names, cb) && push!(store.names[cb], shortname)
        end
    end
    result
end


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

"Returns the object if it exists, returns nothing otherwise."
getindex(store::BucketStore{<:AbstractBackend}, i::String) = getindex(store.backend, joinpath(store.root, i))


function setindex!(store::BucketStore{<:AbstractBackend}, v, i::String)
    # Run checks
    fullpath = joinpath(store.root, i)
    store.permission == :readonly     && return false  # Store does not have permission to create/update objects
    isbucket(store.backend, fullpath) && return false  # i refers to a bucket, not an object
    isobj = isobject(store.backend, fullpath)
    cb, shortname = splitdir(i)
    if isobj
        if store.permission == :limited && !hasobject(store, i)  # Object exists and is not in the store...cannot modify it
            return false
        end
    else
        !isbucket(store.backend, cb) && return false  # Containing bucket does not exist...cannot create an object inside a non-existent bucket
    end

    # Execute
    result = setindex!(store.backend, v, fullpath)
    if result == true
        haskey(store.names, cb) && push!(store.names[cb], shortname)
    end
    result
end


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

"Returns true if the storage backend is on the same machine as the store instance."
islocal(store::BucketStore{<:AbstractBackend}) = islocal(store.backend)

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

end
