#!/usr/bin/env python
#
# This file is part of dbb_gateway
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Prototype program which ingests ATS raw files that were placed
in the delivery directory by dbb_save_ats_raw.py into the Data Backbone
"""

import argparse
import logging
import glob
import os
import sys
import time
import re
import traceback
from datetime import datetime, timedelta
import tarfile
import tempfile
import shutil
import pyfits
import yaml

from lsst.dbb.gwclient import chksum_utils
from lsst.dbb.gateway import db_funcs

# Current code constraints:
# - only 1 data file per tarball
# - only works with Oracle


DEFAULT_MV_TRIES = 5
DUPLICATE_MSG = "Duplicate file"


def read_config(filename):
    """Read config file into dictionary

    Parameters
    ----------
    filename : `str`
        filename, including path, to the config file (yaml format)

    Returns
    -------
    config : `dict`
        Dictionary containing configuration loaded from the yaml file
    """
    with open(filename, "r") as cfgfh:
        config = yaml.load(cfgfh)
    logging.debug(config)
    return config


def parse_args(argv=None):
    """Parse command line, and test for required arguments

    Parameters
    ----------
    argv : `list`
        list of strings containing the command-line arguments

    Returns
    -------
    args : `Namespace`
        The command-line arguments converted into an object with attributes
    """
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true", dest="verbose", required=False,
                        help="Print file level output useful for watching progress")
    parser.add_argument("-d", "--debug", action="store_true", dest="debug", required=False,
                        help="Print very verbose output for debugging")
    parser.add_argument("-c", "--config", dest="config", required=True,
                        help="Config for DBB ingestion from delivery area")
    parser.add_argument("--dryrun", action="store_true", dest="dryrun", required=False,
                        help="If set, does not actually ingest file")
    parser.add_argument("--keep", action="store_true", dest="keep", required=False,
                        help="If set, scratch directory not deleted")

    return parser.parse_args(argv)


def get_camera_name(hdu):
    """Creates short camera name from the INSTRUME fits keyword

    Parameters
    ----------
    hdu : `pyfits.HDU`
        HDU containing the INSTRUME keyword

    Returns
    -------
    short_camera_name: `str`
        A string containing the short camera name
    """
    instrument = hdu.header["instrume"]
    short_camera_name = {"Hyper Suprime-Cam": "HSC",
                         "ATSCAM": "ATS"}

    return short_camera_name[instrument]


def get_observing_nite(hdu):
    """Get observing nite string from fits header or create it from
       date-obs value

    Parameters
    ----------
    hdu : `pyfits.HDU`
        HDU containing the DATE-OBS keyword

    Returns
    -------
    nite : `str`
        A string containing the observing nite (YYYYMMDD)
    """
    if "obs-nite" in hdu.header:
        nite = hdu.header["obs-nite"]
    else:
        # date_obs = "YYYY-MM-DDTHH:MM:SS.S"
        date_obs = hdu.header["date-obs"]
        try:
            obs_datetime = datetime.strptime(date_obs, "%Y-%m-%dT%%H:%M:%S.%f")
            if obs_datetime.hour < 14:
                obs_datetime = obs_datetime - timedelta(days=1)
        except ValueError:
            obs_datetime = datetime.strptime(date_obs, "%Y-%m-%d")

        nite = obs_datetime.strftime("%Y%m%d")
    logging.debug("nite = %s", nite)
    return nite


def get_path_var_names(pattern):
    """Gets variable names from given path pattern

    Parameters
    ----------
    pattern : `str`
        Pattern to use to create path.  Variables are included by
        {name:format}

    Returns
    -------
    patvars : `set`
        A set containing the variable names found in the path pattern
    """
    patvars = re.findall("{([^{}]+)}", pattern)
    patvars = [x.split(":")[0] for x in patvars]
    return set(patvars)


def create_rel_path(filename, pattern):
    """Creates the relative path for the given file using given pattern

    Parameters
    ----------
    filename : `str`
        Name of the file, includes path if needed to open file
    pattern : `str`
        Pattern for the relative path, variables are {varname}

    Returns
    -------
    relpath : `str`
        The pattern string with the variable names replaced by their values
    """
    # get variable names needed to fill pattern
    logging.debug("dir pattern = %s", pattern)
    varnames = get_path_var_names(pattern)
    logging.debug("pat vars = %s", varnames)

    # get values needed in order to replace variables
    calc_vals = {"obsnite": get_observing_nite,
                 "camera": get_camera_name}
    vals = {}
    hdulist = pyfits.open(filename, "readonly")
    for name in varnames:
        if name in calc_vals:
            vals[name] = calc_vals[name](hdulist[0])
        else:
            vals[name] = hdulist[0].header[name]
    hdulist.close()

    # replace variable names with values in pattern
    logging.debug("pat vals = %s", vals)
    relpath = pattern.format(**vals)
    logging.debug("relpath = %s", relpath)

    return relpath


def create_dbb_path(config, src_info):
    """Creates the path for the location of the given file inside the DBB

    Parameters
    ----------
    config : `dict`
        Dictionary containing program configuration options
    src_info : `dict`
        Dictionary containing information about the file, includes
        filename, dataset_type, expected chksum, chksum_type

    Returns
    -------
    dbb_rel_path : `str`
        String containing the path for the file in the DBB, relative to the DBB root
    dbb_fullname : `str`
        String containing the full path for the file in the DBB, including DBB root and filename
    """
    # get relative path inside DBB based upon pattern
    dir_pattern = config["dir_patterns"][src_info["dataset_type"]]
    dbb_rel_path = create_rel_path(src_info["filename"], dir_pattern)

    # create full filename including DBB root and relative path
    dbb_root = config["dbb_root_dir"]
    dbb_path = "%s/%s" % (dbb_root, dbb_rel_path)
    basename = os.path.basename(src_info["filename"])
    dbb_fullname = "%s/%s" % (dbb_path, basename)
    logging.debug("%s -> %s", src_info["filename"], dbb_fullname)

    return dbb_rel_path, dbb_fullname


def save_file_datastore(dbh, config, src_info, dataset_id):
    """Save file into datastore moving file and making DB entries

    Parameters
    ----------
    dbh : `cx_Oracle.Connection`
        Open database connection with write access to DBB tables
    config : `dict`
        Dictionary containing program configuration options
    src_info : `dict`
        Dictionary containing information about file
    dataset_id : `int`
        Dataset id corresponding to dataset information stored in DBB's registry

    Returns
    -------
    dbb_fullname : `str`
        The filename for the file in the DBB with path that includes DBB root
    """
    # figure out where to save it in DBB
    dbb_relpath, dbb_fullname = create_dbb_path(config, src_info)
    logging.debug("dbb_fullname = %s", dbb_fullname)

    # error if file already exists in DBB datastore
    if os.path.exists(dbb_fullname):
        handle_bad_file(config, dbh, src_info, "Consistency error.  Already on disk in DBB")

    # move file to the DBB location
    move_file_to_dbb(src_info["filename"], src_info["chksum"], src_info["chksum_type"],
                     dbb_fullname, DEFAULT_MV_TRIES)

    # save physical and location information in DBB tables
    db_funcs.save_datastore_info(dbh, src_info, dbb_relpath, dataset_id)

    return dbb_fullname


def move_file_to_dbb(src, expected_chksum, chksum_type, dst, max_tries=DEFAULT_MV_TRIES):
    """Move file to its location in the dbb

    Parameters
    ----------
    src : `str`
        Name of the file to move to the DBB, including any necessary path
    expected_chksum : `str`
        Before and after moving, the file's chksum should match this
    chksum_type : `str`
        Which method to use for calculating the chksum
    dst : `str`
        Destination of file including full DBB path and filename
    max_tries : `int`
        Maximum number of times to try to move the file before aborting
    Raises
    ------
    IOError
        Raised if cannot copy file after max_tries
    """
    logging.debug("src = %s", src)
    logging.debug("dst = %s", dst)

    # try a couple times to copy file to dbb directory
    cp_cnt = 1
    copied = False
    while cp_cnt <= max_tries and not copied:
        try:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(src, dst)  # similar to cp -p

            starttime = datetime.now()
            actual_chksum = chksum_utils.calc_chksum(name=dst, chksum_type=chksum_type)
            endtime = datetime.now()
            logging.debug("%s: chksum after move %s (%0.2f secs)", dst, actual_chksum,
                          (endtime - starttime).total_seconds())

            if expected_chksum is None:
                copied = True
            elif expected_chksum != actual_chksum:
                logging.warning("chksum does not match after cp (%s, %s)", src, dst)
                time.sleep(5)
                os.unlink(dst)   # remove bad file from dbb
                cp_cnt += 1
            else:
                copied = True
        except IOError as err:  # Want to retry in case intermittent IOError
            logging.info("Caught %s: %s", type(err).__name__, str(err))
            time.sleep(5)
            if os.path.exists(dst):
                os.unlink(dst)   # remove bad file from dbb
            cp_cnt += 1

    if not copied:
        raise IOError("Cannot cp file (%s->%s)" % (src, dst))

    # if copy successful, remove src to complete the move
    os.unlink(src)


def read_info_file(info_fname):
    """Read the file containing physical and provenance information
       about file known when file was originally saved.

    Parameters
    ----------
    info_fname : `str`
        Name of the file, including any necessary paths

    Returns
    -------
    src_info : `dict`
        Dictionary containing information about file
    """
    with open(info_fname, "r") as infofh:
        src_info = yaml.load(infofh)
    logging.debug("src_info = %s", src_info)
    return src_info


def handle_tarball(tar_filename, scratch_dir, dbh, config):
    """Performs steps necessary for each file

    Parameters
    ----------
    tar_filename : `str`
        The tarball filname including the path to the delivery area.
    scratch_dir : `str`
        The scratch directory where the tarball should be extracted
    dbh : `cx_Oracle.Connection`
        Open database connection with write access to DBB tables
    config : `dict`
        Dictionary containing program configuration options

    Raises
    ------
    ValueError
        Raised when an internal consistency check failed because two
        copies of a chksum value do not match
    """
    dbb_fullname = None
    src_info = None

    try:
        logging.debug("tar_filename = %s", tar_filename)

        if not os.path.exists(tar_filename):
            logging.warning("tarball does not exist = %s.   Skipping", tar_filename)
            return

        untar2dir(tar_filename, scratch_dir)

        os.chdir(scratch_dir)
        filename, info_fname, digest_fname = get_filenames()

        with open(digest_fname, "r") as digestfh:
            digest = read_digest(digestfh)
        logging.debug("digest = %s", digest)

        integrity_check(info_fname, digest[info_fname])
        integrity_check(filename, digest[filename])

        src_info = read_info_file(info_fname)
        src_info["tar_filename"] = tar_filename

        # Internal consistency test
        if digest[filename] != src_info["chksum"]:
            raise ValueError("Internal consistency check failed. chksum values do not match for %s" %
                             filename)

        process_id = db_funcs.get_registration_process_id(dbh, src_info['uuid'])
        if process_id is None:
            process_id = db_funcs.save_registration_info(dbh, src_info)
        logging.debug("Registration process id = %s", process_id)
        src_info["process_id"] = process_id

        if db_funcs.filename_exists_in_dbb(dbh, src_info["filename"]):
            logging.debug("Non-unique filename = %s", src_info["filename"])
            handle_bad_file(config, dbh, src_info, DUPLICATE_MSG)
        else:
            start_time = datetime.now()
            dataset_id = db_funcs.register_file_data(dbh, src_info)
            logging.debug("%s: gathering and registering file data (%0.2f secs)",
                          src_info["filename"], (datetime.now() - start_time).total_seconds())

            dbb_fullname = save_file_datastore(dbh, config, src_info, dataset_id)
            logging.debug("%s: fullname in dbb %s", src_info["filename"], dbb_fullname)

            logging.debug("%s: success.  committing to db", src_info["filename"])
            os.remove(tar_filename)
            db_funcs.save_end_time(dbh, process_id)
            dbh.commit()

    except SyntaxError:   # assuming this is program problem and not data problem
        raise
    except Exception as err:
        (extype, exvalue, trback) = sys.exc_info()
        print("******************************")
        print("Error: %s" % tar_filename)
        traceback.print_exception(extype, exvalue, trback, file=sys.stdout)
        print("******************************")

        # undo any pending database changes for this file
        dbh.rollback()

        # if error happened after copying file to DBB location, need to remove it from DBB
        if dbb_fullname is not None:
            logging.warning("Removing bad file from dbb = %s", dbb_fullname)
            os.remove(dbb_fullname)

        handle_bad_file(config, dbh, src_info, "%s: %s" % (type(err).__name__, str(err)))


def move_bad_file(config, tar_filename):
    """Move tarball into bad file directory on disk

    Parameters
    ----------
    config : `dict`
        Dictionary containing program configuration options
    tar_filename : `str`
        filename of tarball including delivery path

    Returns
    -------
    relpath : `str`
        The relative path inside the bad file directory where the
        bad file was saved
    """
    today = datetime.now()
    relpath = "%04d/%02d" % (today.year, today.month)
    newpath = "%s/%s" % (config["bad_file_dir"], relpath)
    destbad = "%s/%s" % (newpath, os.path.basename(tar_filename))

    if os.path.exists(destbad):
        logging.warning("bad file already exists (%s)", destbad)
        os.remove(destbad)

    # make directory in "bad file" area and move file there
    os.makedirs(newpath, exist_ok=True)
    shutil.move(tar_filename, destbad)
    return relpath


def handle_bad_file(config, dbh, src_info, msg):
    """ Perform steps required by any bad file

    Parameters
    ----------
    config : `dict`
        Dictionary containing program configuration options
    dbh : `cx_Oracle.Connection`
        Open database connection with write access to DBB tables
    src_info : `dict`
        Dictionary containing information about file
    msg: `str`
        String explaining cause of rejection
    """
    dbh.rollback()  # undo any db changes for this file

    logging.info("msg = %s", msg)
    logging.debug("src_info = %s", src_info)
    logging.debug("dataset_type = %s", src_info["dataset_type"])

    bad_info = {}
    if src_info is not None:
        bad_info["delivery_date"] = datetime.fromtimestamp(src_info["timestamp"])
    else:
        bad_info["delivery_date"] = datetime.fromtimestamp(os.path.getmtime(src_info["tar_filename"]))
    logging.debug("delivery_date = %s", bad_info["delivery_date"])
    bad_info["uniq_filename"] = os.path.basename(src_info["tar_filename"])
    bad_info["disk_usage"] = os.path.getsize(src_info["tar_filename"])
    bad_info["rejected_date"] = datetime.now()
    bad_info["rejected_msg"] = msg

    badpath = move_bad_file(config, src_info["tar_filename"])
    bad_info["relpath"] = badpath

    db_funcs.save_bad_file_db(dbh, bad_info, src_info)


def get_list_tarballs(delivery_dir):
    """Create list of tarballs that contain files that need to be put into dbb

    Parameters
    ----------
    delivery_dir : `str`
        directory containing the tarballs to be ingested into the dbb

    Returns
    -------
    tarballs : `list`
        List of tarballs found in the delivery directory (sorted by modification date)

    Raises
    ------
    FileNotFoundError
        Raised when delivery directory does not exist
    """
    if not os.path.exists(delivery_dir):
        raise FileNotFoundError("Delivery directory does not exist: %s" % delivery_dir)
    # order the returned list so ingested in order of delivery
    filenames = next(os.walk(delivery_dir))[2]
    tarballs = []
    for fname in filenames:
        if fname.endswith(".tar"):
            tarballs.append("%s/%s" % (delivery_dir, fname))
    return sorted(tarballs, key=os.path.getmtime)


def read_digest(infh, delim="\t"):
    """ Read and parse the checksum digest file

    Parameters
    ----------
    infh : ``
        Object with readlines capability that contains digest information
        Digest line format similar to md5sum digest:
            chksum<delim>filename
        where filename does not include path
    delim : `str`
        character used to split line in file

    Returns
    -------
    digest : `dict`
        Filename to expected chksum value mapping
    """
    digest = {}
    for line in infh.readlines():
        line = line.strip()
        vals = [x.strip() for x in line.split(delim)]
        digest[vals[1]] = vals[0]
    return digest


def untar2dir(tarfilename, outputdir):
    """Untars tarball to a specific directory.

    Parameters
    ----------
    tarfilename : `str`
        Name of tarball including path
    outputdir : `str`
        Path into which the contents of the tarball should be extracted
    """
    if tarfilename.endswith(".gz"):
        mode = "r:gz"
    else:
        mode = "r"

    with tarfile.open(tarfilename, mode) as tar:
        tar.extractall(outputdir)


def get_filenames():
    """ Get filenames that were just extracted from tarball

    Returns
    -------
    filename : `str`
        filename of the raw file to be stored in the DBB
    info_fname : `str`
        filename of the yaml file containing information about
        raw file including provenance
    digest_fname : `str`
        filename of the text file containing chksum information
        about the raw and info files

    Notes
    -----
    Assumes already in directory where the only files are those
    extracted from the tarball with no subdirectory structure
    """
    filename = None
    info_fname = None
    digest_fname = None

    files = glob.glob("*")
    for fname in files:
        if fname.endswith(".info"):
            info_fname = fname
        elif fname.endswith(".digest"):
            digest_fname = fname
        else:
            filename = fname

    logging.debug(filename)
    return filename, info_fname, digest_fname


def integrity_check(filename, expected, chksum_type="md5", blksize=chksum_utils.DEFAULT_BLKSIZE):
    """ Compare chksums between expected and computed actual

    Parameters
    ----------
    filename : `str`
        Name of file on which to test chksum, includes path if needed
    expected : `str`
        Chksum string expected for the given file
    chksum_type : `str`
        Name of method to use for calculating the chksum
    blksize : `int`
        Number of bytes to read in a single chunk from the file

    Raises
    ------
    ValueError if expected and actual chksums do not match
    """
    logging.info("Integrity checking file: %s", filename)
    actual = chksum_utils.calc_chksum(name=filename, chksum_type=chksum_type, blksize=blksize)
    logging.debug("Expected chksum = %s    Actual chksum = %s", expected, actual)
    if expected != actual:
        raise ValueError("%s chksums (%s) do not match" % (filename, chksum_type))

    logging.info("%s passed integrity check", filename)


def main(argv):
    """ Program entry point

    Parameters
    ----------
    argv : `list`
        list of command-line arguments to pass to argparse
    """
    cwd = os.getcwd()
    args = parse_args(argv)

    # set logging configuration
    logging.basicConfig(format="%(levelname)s::%(asctime)s::%(message)s", datefmt="%m/%d/%Y %H:%M:%S")
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    elif args.verbose:
        logging.getLogger().setLevel(logging.INFO)

    logging.debug("Cmdline args = %s", args)
    config = read_config(args.config)
    tarballs = get_list_tarballs(config["delivery_dir"])
    logging.debug("tarballs = %s", tarballs)

    if tarballs:
        dbh = db_funcs.open_db_connection(config["db"])
        print(type(dbh))
        for tar_filename in tarballs:
            dirprefix = os.path.splitext(os.path.basename(tar_filename))[0]
            scratch_dir = tempfile.mkdtemp(prefix=dirprefix, dir=config["scratch_root"])
            logging.debug("Scratch directory = %s", scratch_dir)

            try:
                handle_tarball(tar_filename, scratch_dir, dbh, config)
            finally:
                os.chdir(cwd)
                if not args.keep and dirprefix in scratch_dir:
                    shutil.rmtree(scratch_dir)
        dbh.close()
    else:
        logging.info("0 tarballs in delivery directory")


if __name__ == "__main__":
    main(sys.argv[1:])
