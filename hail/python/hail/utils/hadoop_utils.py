from hail.utils.java import Env
from hail.typecheck import typecheck, enumeration
from typing import Dict, List


@typecheck(path=str,
           mode=enumeration('r', 'w', 'x', 'rb', 'wb', 'xb'),
           buffer_size=int)
def hadoop_open(path: str, mode: str = 'r', buffer_size: int = 8192):
    """Open a file through the Hadoop filesystem API. Supports distributed
    file systems like hdfs, gs, and s3.

    Warning
    -------
    Due to an implementation limitation, :func:`hadoop_open` may be quite
    slow for large data sets (anything larger than 50 MB).

    Examples
    --------
    Write a Pandas DataFrame as a CSV directly into Google Cloud Storage:

    >>> with hadoop_open('gs://my-bucket/df.csv', 'w') as f: # doctest: +SKIP
    ...     pandas_df.to_csv(f)

    Read and print the lines of a text file stored in Google Cloud Storage:

    >>> with hadoop_open('gs://my-bucket/notes.txt') as f: # doctest: +SKIP
    ...     for line in f:
    ...         print(line.strip())

    Write two lines directly to a file in Google Cloud Storage:

    >>> with hadoop_open('gs://my-bucket/notes.txt', 'w') as f: # doctest: +SKIP
    ...     f.write('result1: %s\\n' % result1)
    ...     f.write('result2: %s\\n' % result2)

    Unpack a packed Python struct directly from a file in Google Cloud Storage:

    >>> from struct import unpack
    >>> with hadoop_open('gs://my-bucket/notes.txt', 'rb') as f: # doctest: +SKIP
    ...     print(unpack('<f', bytearray(f.read())))

    Notes
    -----
    The supported modes are:

     - ``'r'`` -- Readable text file (:class:`io.TextIOWrapper`). Default behavior.
     - ``'w'`` -- Writable text file (:class:`io.TextIOWrapper`).
     - ``'x'`` -- Exclusive writable text file (:class:`io.TextIOWrapper`).
       Throws an error if a file already exists at the path.
     - ``'rb'`` -- Readable binary file (:class:`io.BufferedReader`).
     - ``'wb'`` -- Writable binary file (:class:`io.BufferedWriter`).
     - ``'xb'`` -- Exclusive writable binary file (:class:`io.BufferedWriter`).
       Throws an error if a file already exists at the path.


    The provided destination file path must be a URI (uniform resource identifier).

    .. caution::

        These file handles are slower than standard Python file handles. If you
        are writing a large file (larger than ~50M), it will be faster to write
        to a local file using standard Python I/O and use :func:`.hadoop_copy`
        to move your file to a distributed file system.

    Parameters
    ----------
    path : :obj:`str`
        Path to file.
    mode : :obj:`str`
        File access mode.
    buffer_size : :obj:`int`
        Buffer size, in bytes.

    Returns
    -------
        Readable or writable file handle.
    """
    return Env.fs().open(path, mode, buffer_size)


@typecheck(src=str,
           dest=str)
def hadoop_copy(src, dest):
    """Copy a file through the Hadoop filesystem API.
    Supports distributed file systems like hdfs, gs, and s3.

    Examples
    --------
    Copy a file from Google Cloud Storage to a local file:

    >>> hadoop_copy('gs://hail-common/LCR.interval_list',
    ...             'file:///mnt/data/LCR.interval_list') # doctest: +SKIP

    Notes
    ----

    Try using :func:`.hadoop_open` first, it's simpler, but not great
    for large data! For example:

    >>> with hadoop_open('gs://my_bucket/results.csv', 'w') as f: #doctest: +SKIP
    ...     pandas_df.to_csv(f)

    The provided source and destination file paths must be URIs
    (uniform resource identifiers).

    Parameters
    ----------
    src: :obj:`str`
        Source file URI.
    dest: :obj:`str`
        Destination file URI.
    """
    return Env.fs().copy(src, dest)


def hadoop_exists(path: str) -> bool:
    """Returns ``True`` if `path` exists.

    Parameters
    ----------
    path : :obj:`str`

    Returns
    -------
    :obj:`.bool`
    """
    return Env.fs().exists(path)


def hadoop_is_file(path: str) -> bool:
    """Returns ``True`` if `path` both exists and is a file.

    Parameters
    ----------
    path : :obj:`str`

    Returns
    -------
    :obj:`.bool`
    """
    return Env.fs().is_file(path)


def hadoop_is_dir(path: str) -> bool:
    """Returns ``True`` if `path` both exists and is a directory.

    Parameters
    ----------
    path : :obj:`str`

    Returns
    -------
    :obj:`.bool`
    """
    return Env.fs().is_dir(path)


def hadoop_stat(path: str) -> Dict:
    """Returns information about the file or directory at a given path.

    Notes
    -----
    Raises an error if `path` does not exist.

    The resulting dictionary contains the following data:

    - is_dir (:obj:`bool`) -- Path is a directory.
    - size_bytes (:obj:`int`) -- Size in bytes.
    - size (:obj:`str`) -- Size as a readable string.
    - modification_time (:obj:`str`) -- Time of last file modification.
    - owner (:obj:`str`) -- Owner.
    - path (:obj:`str`) -- Path.

    Parameters
    ----------
    path : :obj:`str`

    Returns
    -------
    :obj:`Dict`
    """
    return Env.fs().stat(path)


def hadoop_ls(path: str) -> List[Dict]:
    """Returns information about files at `path`.

    Notes
    -----
    Raises an error if `path` does not exist.

    If `path` is a file, returns a list with one element. If `path` is a
    directory, returns an element for each file contained in `path` (does not
    search recursively).

    Each dict element of the result list contains the following data:

    - is_dir (:obj:`bool`) -- Path is a directory.
    - size_bytes (:obj:`int`) -- Size in bytes.
    - size (:obj:`str`) -- Size as a readable string.
    - modification_time (:obj:`str`) -- Time of last file modification.
    - owner (:obj:`str`) -- Owner.
    - path (:obj:`str`) -- Path.

    Parameters
    ----------
    path : :obj:`str`

    Returns
    -------
    :obj:`List[Dict]`
    """
    return Env.fs().ls(path)


def copy_log(path: str) -> None:
    """Attempt to copy the session log to a hadoop-API-compatible location.

    Examples
    --------
    Specify a manual path:

    >>> hl.copy_log('gs://my-bucket/analysis-10-jan19.log')  # doctest: +SKIP
    INFO: copying log to 'gs://my-bucket/analysis-10-jan19.log'...

    Copy to a directory:

    >>> hl.copy_log('gs://my-bucket/')  # doctest: +SKIP
    INFO: copying log to 'gs://my-bucket/hail-20180924-2018-devel-46e5fad57524.log'...

    Notes
    -----
    Since Hail cannot currently log directly to distributed file systems, this
    function is provided as a utility for offloading logs from ephemeral nodes.

    If `path` is a directory, then the log file will be copied using its
    base name to the directory (e.g. ``/home/hail.log`` would be copied as
    ``gs://my-bucket/hail.log`` if `path` is ``gs://my-bucket``.

    Parameters
    ----------
    path: :obj:`str`
    """
    Env.fs().copy_log(path)


def hadoop_delete(path: str):
    """Attempt to delete a file through hadoop-API

    """
    Env.fs().delete(path, True)
