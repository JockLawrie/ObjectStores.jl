"""
Defines behaviour of bucket stores (concrete subtypes of AbstractBucketStore).

Also defines fields that are common to all bucket stores.
"""
module AbstractBucketStores

export AbstractBucketStore,
       @add_BucketStore_common_fields,                    # Used in constructor of concrete subtypes
       listcontents, createbucket!, deletebucket!,        # Buckets
       getindex, setindex!, delete!,                      # Objects
       islocal, isbucket, hasbucket, isobject, hasobject  # Conveniences

import Base.setindex!, Base.getindex, Base.delete!

abstract type AbstractBucketStore end

const type2module = Dict{DataType, Module}()  # Enables concrete types to access behaviour of AbstractBucketStore


################################################################################
# Constructor

"""
Adds fields that are common to all concrete subtypes of AbstractBucketStore.

Use this macro when defining a concrete subtype of AbstractBucketStore.

For example, the LocalDiskStore is defined as follows (see the LocalDiskStores.jl package):

  struct LocalDiskStore <: AbstractBucketStore
      @add_BucketStore_common_fields  # Fields that all bucket stores require
  end

You can also add fields that are specific to your type.
For example:

  struct MyStore <: AbstractBucketStore
      @add_BucketStore_common_fields  # Fields that all bucket stores require
      otherfield1::Int                # Field that is specific to MyStore
      otherfield2::String             # Field that is specific to MyStore
  end
"""
macro add_BucketStore_common_fields()
    return esc(:(
        permission::Symbol;                # One of :readonly, :limited, :unlimited
        root::String;                      # Name of root bucket, which contains all buckets and objects in the store.
        names::Dict{String, Set{String}};  # bucketname => Set(bucket_names..., object_names...). bucketname excludes root. Names exclude root/bucketname.
        bucketnames::Set{String};)         # Names of buckets created by the store (not all buckets in names were created by the store).
    )
end


"""
Constructor for concrete subtypes.

Signature is T(permission, root, type_specific_args...)

Examples:
- From the LocalDiskStores package:  LocalDiskStore(:limited, "/tmp/rootbucket")
"""
function (::Type{T})(permission::Symbol, root::String, type_specific_args...) where {T <: AbstractBucketStore}
    !(T <: AbstractBucketStore) && error("Store is not a subtype of AbstractBucketStore")
    !in(permission, Set([:readonly, :limited, :unlimited])) && error("Permission must be one of :readonly, :limited, :unlimited")
    type2module[T] = parentmodule(T)  # Register the package that defines T
    newstore = T(permission, root, Dict{String, Set{String}}(root => Set{String}()), Set{String}(), type_specific_args...)
    isobject(newstore, root) && error("Root already exists as an object. Cannot use it as a bucket.")
    if !isbucket(newstore, root)      # Root does not exist...create it
        if permission == :readonly
            error("Root bucket does not exist and could not be created because permission is read-only.")
        else
            ok = createbucket!(newstore, root, true)  # 3rd arg is true because bucketname is root
            if !ok
                error("Root bucket does not exist and could not be created.")
            end
        end
    end
    newstore
end

################################################################################
# API: Buckets

"""
Returns a list (Vector) of the names of the buckets and objects contained in the given bucket if it exists, returns nothing otherwise.

The list includes buckets and objects not created by the BucketStore instance.

If a bucket name is not supplied, the contents of the root bucket are given.
"""
function listcontents(store::T, bucketname::String) where {T <: AbstractBucketStore}
    m = type2module[typeof(store)]
    m._listcontents(store, joinpath(store.root, bucketname))
end


function listcontents(store::T) where {T <: AbstractBucketStore}
    m = type2module[typeof(store)]
    m._listcontents(store, store.root)
end


"""
Modified: store.names

Return true if all checks pass, else return false.

If bucketisroot then create the root bucket.
"""
function createbucket!(store::T, bucketname::String, bucketisroot::Bool=false) where {T <: AbstractBucketStore}
    result = false
    if store.permission != :readonly  # Store has write permission
        m        = type2module[typeof(store)]
        fullpath = bucketisroot ? bucketname : joinpath(store.root, bucketname)
        result   = m._createbucket!(store, fullpath)
        if result == true
            push!(store.bucketnames, bucketname)
            store.names[bucketname] = Set{String}()
            if !bucketisroot
                cb, shortname = splitdir(bucketname)
                if cb == ""  # bucketname is a member of the root bucket
                    cb = store.root
                end
                haskey(store.names, cb) && push!(store.names[cb], shortname)
            end
        end
    end
    result
end


"""
Modified: store.names

Return true if all checks pass, else return false.

If bucketisroot then delete the root bucket.
"""
function deletebucket!(store::T, bucketname::String, bucketisroot::Bool=false) where {T <: AbstractBucketStore}
    result = false
    if (store.permission == :limited && hasbucket(store, bucketname)) || store.permission == :unlimited
        m        = type2module[typeof(store)]
        fullpath = bucketisroot ? bucketname : joinpath(store.root, bucketname)
        result   = m._deletebucket!(store, fullpath)
        if result == true
            hasbucket(store, bucketname)    && pop!(store.bucketnames, bucketname)
            haskey(store.names, bucketname) && delete!(store.names, bucketname)
            if !bucketisroot
                cb, shortname = splitdir(bucketname)
                if cb == ""  # bucketname is a member of the root bucket
                    cb = store.root
                end
                haskey(store.names, cb) && pop!(store.names[cb], shortname)
            end
        end
    end
    result
end


################################################################################
# API: Objects

"Returns the object if it exists, returns nothing otherwise."
function getindex(store::T, i::String) where {T <: AbstractBucketStore}
    m = type2module[typeof(store)]
    m._getindex(store, joinpath(store.root, i))
end


function setindex!(store::T, v, i::String) where {T <: AbstractBucketStore}
    # Run checks
    store.permission == :readonly && return false  # Store does not have permission to create/update objects
    fullpath = joinpath(store.root, i)
    m        = type2module[typeof(store)]
    m._isbucket(store, fullpath)  && return false  # i refers to a bucket, not an object
    if m._isobject(store, fullpath)
        if store.permission == :limited && !hasobject(store, i)  # Object exists and is not in the store...cannot modify it
            return false
        end
    else
        cb, shortname = splitdir(fullpath)
        !m._isbucket(store, cb) && return false  # Containing bucket does not exist...cannot create an object inside a non-existent bucket
    end

    # Execute
    result = m._setindex!(store, v, fullpath)
    cb, shortname = splitdir(i)
    result == true && haskey(store.names, cb) && push!(store.names[cb], shortname)
    result
end


function delete!(store::T, i::String) where {T <: AbstractBucketStore}
    result = false
    if (store.permission == :limited && hasobject(store, i)) || store.permission == :unlimited
        m      = type2module[typeof(store)]
        result = m._delete!(store, joinpath(store.root, i))
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
function islocal(store::T) where {T <: AbstractBucketStore}
    m = type2module[typeof(store)]
    m._islocal(store)
end

"Returns true if name refers to a bucket."
function isbucket(store::T, name::String) where {T <: AbstractBucketStore}
    m = type2module[typeof(store)]
    m._isbucket(store, joinpath(store.root, name))
end

"Returns true if name refers to an object."
function isobject(store::T, name::String) where {T <: AbstractBucketStore}
    m = type2module[typeof(store)]
    m._isobject(store, joinpath(store.root, name))
end

"Returns true if the bucket is in the store."
hasbucket(store::T, bucketname::String) where {T <: AbstractBucketStore} = in(bucketname, store.bucketnames)

"Returns true if the bucket is in the store."
function hasobject(store::T, objectname::String) where {T <: AbstractBucketStore}
    cb, shortname = splitdir(objectname)
    haskey(store.names, cb) && in(shortname, store.names[cb])
end

end
