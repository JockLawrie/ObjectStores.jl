module LocalDiskStores


export LocalDiskStore,
       listcontents, createbucket!, deletebucket!,  # Buckets
       getindex, setindex!, delete!,  # Objects
       islocal, isbucket, isobject    # Conveniences


using ..AbstractBackend


struct LocalDiskStore <: AbstractBackend end


################################################################################
# Buckets

"If fullpath is a bucket, return a list of the bucket's contents, else return nothing."
function listcontents(backend::LocalDiskStore, fullpath::String)
    !isbucket(backend, fullpath) && return nothing
    readdir(fullpath)
end


"""
Returns true if bucket is successfully created, false otherwise.

Create bucket if:
1. It doesn't already exist (as either a bucket or an object), and
2. The containing bucket exists.
"""
function createbucket!(backend::LocalDiskStore, fullpath::String)
    !isbucket(backend, fullpath) && return false
    cb, bktname = splitdir(bktname)
    !isbucket(backend, cb) && return false  # Containing bucket doesn't exist
    mkdir(fullpath)
    true
end


"""
Returns true if bucket is successfully deleted, false otherwise.

Delete bucket if:
1. fullpath is a bucket name (the bucket exists), and
2. The bucket is empty.
"""
function deletebucket!(backend::LocalDiskStore, fullpath::String)
    contents = listcontents(backend, fullpath)
    contents == nothing && return false  # fullpath is not a bucket
    !isempty(contents)  && return false  # Bucket is not empty
    rmdir(fullpath)
    true
end


################################################################################
# Objects

"Return object if fullpath refers to an object, else return nothing."
function getindex(backend::LocalDiskStore, fullpath::String) 
    !isobject(backend, fullpath) && return nothing
    read(fullpath)
end


"If fullpath is an object, set fullpath = v and return true, else return false."
function setindex!(backend::LocalDiskStore, v, fullpath::String)
    write(fullpath, v)
    true
end


"If object exists, delete it and return true, else return false."
function delete!(backend::LocalDiskStore, fullpath::String)
    !isobject(backend, fullpath) && return false
    rm(fullpath)
    true
end


################################################################################
# Conveniences

islocal(backend::LocalDiskStore) = true

isbucket(backend::LocalDiskStore, fullpath::String) = isdir(fullpath)

isobject(backend::LocalDiskStore, fullpath::String) = isfile(fullpath)

end
