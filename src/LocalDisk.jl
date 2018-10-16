################################################################################

islocal(backend::LocalDisk) = true

isbucket(backend::LocalDisk, fullpath::String) = isfile(fullpath)

isobject(backend::LocalDisk, fullpath::String) = isdir(fullpath)

function listbucket(backend::LocalDisk, fullpath::String)
    !isdir(fullpath) && return nothing
    readdir(fullpath)
end

createbucket!(backend::LocalDisk, fullpath::String)  # Create if cb exists and fullpath doesn't already exist (as either a bucket or an object)

deletebucket!(backend::LocalDisk, fullpath::String)  # Delete if fullpath is a bucket and non-empty

setindex!(backend::LocalDisk, v, fullpath::String)  # Set if fullpath is not a bucket

getindex!(backend::LocalDisk, fullpath::String)  # Return nothing if object does not exist or if fullpath is a bucket

delete!(backend::LocalDisk, fullpath::String)  # If object exists, delete it

################################################################################

"""
prefix::String is a directory.
"""
struct LocalDisk <: AbstractStorage
    prefix::String          # Example: prefix = "/tmp/" implies storage["mydata"] is stored at "/tmp/mydata".
    filenames::Set{String}  # Set of keys that are file names (full paths). Prevents the deletion of files that weren't created via LocalDisk.
    dirnames::Set{String}   # Set of directories created using the LocalDisk API.
    function LocalDisk(prefix, filenames, dirnames)
        if prefix[end] != '/'
            newprefix = "$(prefix)/"  # Append trailing '/'
            if in(prefix, dirnames)
                pop!(dirnames, prefix)
                push!(dirnames, newprefix)
            end
            prefix = newprefix
        end
        mkpath(prefix)
        new(prefix, filenames, dirnames)
    end
end

function LocalDisk(prefix::String)
    dirnames  = isdir(prefix) ? Set{String}() : Set{String}([prefix])  # If prefix exists, do not let it be deleted
    filenames = Set{String}()
    mkpath(prefix)
    LocalDisk(prefix, filenames, dirnames)
end


hasfile(s::LocalDisk, k::String) = in(k, s.filenames)
hasdir(s::LocalDisk, k::String)  = in(k, s.dirnames)


function setindex!(s::LocalDisk, v, i::String)
    # Checks
    i[end] == '/' && error("A directory cannot be a LocalDisk key. Please specify a file name.")
    occursin("..", i) && error("Key name cannot contain '..'.")
    fullpath = joinpath(s.prefix, i)
    isdir(fullpath)  && error("$(fullpath)/ is an existing directory, it cannot be a LocalDisk key.")
    #isfile(fullpath) && !hasfile(s, fullpath) && error("Cannot overwrite an existing file that is not a key of this LocalDisk instance.")

    # Push dirnames of all directories between s.prefix and fulldir.
    fulldir   = "$(dirname(fullpath))/"
    suffixdir = replace(fulldir, s.prefix => "")
    dirparts  = split(suffixdir, '/')
    shortdir  = s.prefix
    push!(s.dirnames, s.prefix)
    for dirpart in dirparts
        dirpart == "" && continue
        shortdir = "$(joinpath(shortdir, dirpart))/"     # Intermediate directory (between prefix and fulldir)
        !isdir(shortdir) && push!(s.dirnames, shortdir)  # Directory does not already exist
    end

    # Write data
    !isfile(fullpath) && push!(s.filenames, fullpath)    # File does not already exist
    mkpath(fulldir)  # Ensure directory exists
    write(fullpath, v)
end


function getindex(s::LocalDisk, i::String)
    fullpath = joinpath(s.prefix, i)
    isdir(fullpath)   && error("Directory cannot be a key of a LocalDisk instance.")
    !isfile(fullpath) && error("File does not exist.")
    #!hasfile(s, fullpath) && error("File is not a key of LocalDisk.")
    read(fullpath)
end


"""
If i is a filename, delete the file.
Else if i is a directory name, and if the directory is empty, delete the directory.
Else do nothing.
"""
function delete!(s::LocalDisk, i::String)
    fullpath = joinpath(s.prefix, i)
    delete_fullpath!(s, fullpath)
end


"Recursively delete all contents of the directory and the directory itself."
function deleteall!(s::LocalDisk, i::String)
    fullpath = joinpath(s.prefix, i)
    deleteall_fullpath!(s, fullpath)
end


"Delete all content that has been created by this LocalDisk instance."
function deleteall!(s::LocalDisk)
    deleteall!(s, s.prefix)
end


################################################################################
# Non-exported functions
"""
If fullpath is a filename, delete the file.
Else if fullpath is a directory name, and if the directory is empty, delete the directory.
Else do nothing.
"""
function delete_fullpath!(s::LocalDisk, fullpath::String)
    occursin("..", fullpath) && error("Key name cannot contain '..'.")
    if isfile(fullpath)
        if hasfile(s, fullpath)
            rm(fullpath)
            pop!(s.filenames, fullpath)
        else
            error("Cannot remove file that was not created by this LocalDisk instance.")
        end
    elseif isdir(fullpath)
        !hasdir(s, fullpath) && error("Cannot remove directory that was not created by this LocalDisk instance.")
        contents = readdir(fullpath)   # Vector{String}
        if isempty(contents)           # Only delete an empty directory
            rm(fullpath)
            pop!(s.dirnames, fullpath)
        else
            error("Cannot delete directory $(fullpath) because it is not empty.")
        end
    end
end


"Recursively delete all contents of the directory and the directory itself."
function deleteall_fullpath!(s::LocalDisk, fullpath::String)
    !isdir(fullpath) && error("Cannot deleteall $(fullpath) because it is not an existing directory.")
    if fullpath[end] != '/'
        fullpath = "$(fullpath)/"
    end
    contents = readdir(fullpath)         # Vector{String}
    for x in contents
        x_full = joinpath(fullpath, x)
        if hasfile(s, x_full)
            delete_fullpath!(s, x_full)  # Delete file
        elseif isdir(x_full)
            deleteall!(s, x_full)        # Recursively delete contents
        end
    end
    fulldir = "$(dirname(fullpath))/"
    delete_fullpath!(s, fulldir)         # Delete directory
end
