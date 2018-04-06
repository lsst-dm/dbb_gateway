# This file is part of dbb_gateway.
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

"""Helper functions for interaction with Data Backbone tables in
   Oracle database
"""
import logging
from datetime import datetime
import cx_Oracle

# Current code constraints:
# - Place-holder until Gen3 Butler code is ready
# - only works with Oracle


def open_db_connection(db_config):
    """Save to the database base information used for provenance

    Parameters
    ----------
    db_config : `dict`
        dictionary containing values needed to connect to DB

    Returns
    -------
    dbh : `cx_Oracle.Connection`
        Open database connection to consolidated DB
    """
    dsn = cx_Oracle.makedsn(**db_config)
    logging.debug("dsn = %s", dsn)
    dbh = cx_Oracle.connect(dsn=dsn)
    return dbh


def get_sequence_val(dbh, seqname):
    """Get the next value from a sequence

    Parameters
    ----------
    dbh : `cx_Oracle.Connection`
        Open database connection with write access to DBB tables
    seqname : `str`
        Name of sequence

    Returns
    -------
    nextval : `int`
        Next value from the specified sequence
    """
    curs = dbh.cursor()
    sql = "select %s.nextval from DUAL" % seqname
    curs.execute(sql)
    nextval = curs.fetchone()[0]
    return nextval


def save_datastore_info(dbh, src_info, relpath, dataset_id):
    """Save the physical information about the file

    Parameters
    ----------
    dbh : `cx_Oracle.Connection`
        Open database connection with write access to DBB tables
    src_info : `dict`
        Dictionary containing information about the file
    relpath : `str`
        Relative path of the file in the DBB
    dataset_id : `int`
        Dataset ID corresponding to this file (needed for table joins)
    """
    curs = dbh.cursor()
    sql = ("insert into datastore_dbb (dataset_id, filename, relpath, filesize, chksum, chksum_type) "
           "values (:dataset_id, :filename, :relpath, :filesize, :chksum, :chksum_type)")
    logging.debug("sql = %s", sql)
    curs.execute(sql, {"dataset_id": dataset_id,
                       "filename": src_info["filename"],
                       "relpath": relpath,
                       "filesize": src_info["filesize"],
                       "chksum": src_info["chksum"],
                       "chksum_type": src_info["chksum_type"]})


def filename_exists_in_dbb(dbh, filename):
    """Checks whether filename already exists in DBB

    Parameters
    ----------
    dbh : `cx_Oracle.Connection`
        Open database connection with write access to DBB tables
    filename : `str`
        Name of file (no path) to search DBB for

    Returns
    -------
    exists : `bool`
        True if filename exists in DBB, otherwise False
    """
    curs = dbh.cursor()
    sql = "select filename from datastore_dbb where filename=:filename"
    logging.debug("sql = %s", sql)
    curs.execute(sql, {"filename": filename})
    row = curs.fetchone()
    return row is not None


def register_file_data(dbh, src_info):
    """Save dataset entries in database

    Parameters
    ----------
    dbh : `cx_Oracle.Connection`
        Open database connection with write access to DBB tables
    src_info : `dict`
        Dictionary containing information about the file

    Returns
    -------
    dataset_id : `int`
        New dataset id corresponding to dataset information stored in DBB's registry
    """
    # future version of this code is Gen3 Butler Registry code
    dataset_id = get_sequence_val(dbh, "DATASET_SEQ")
    curs = dbh.cursor()
    sql = "insert into dataset (id, wgb_process_id, dataset_type) values (:id, :process_id, :dataset_type)"
    logging.debug("sql = %s", sql)
    curs.execute(sql, {"id": dataset_id,
                       "process_id": src_info["process_id"],
                       "dataset_type": src_info["dataset_type"]})
    return dataset_id


def save_end_time(dbh, process_id):
    """Update the end_time in the process table for particular process id

    Parameters
    ----------
    dbh : `cx_Oracle.Connection`
        Open database connection with write access to DBB tables
    process_id : `int`
        ID of the process for which to update the end time
    """
    curs = dbh.cursor()
    sql = "update process set end_time=SYSTIMESTAMP where id=:id"
    logging.debug("sql = %s", sql)
    curs.execute(sql, {"id": process_id})


def save_bad_file_db(dbh, bad_info, src_info):
    """Save tracking information about a file stored in the bad file area

    Parameters
    ----------
    dbh : `cx_Oracle.Connection`
        Open database connection with write access to DBB tables
    bad_info : `dict`
        Dictionary containing extra information about the file including
        why it was rejected, where it is stored in the bad file area, etc
        required keys = uniq_filename, relpath, disk_usage, rejected_msg, rejected_date
    src_info : `dict`
        Dictionary containing information about the original file
        required keys = filename, dataset_type, filesize, chksum, chksum_type
    """
    logging.debug("save_bad_file_db: bad_info = %s", bad_info)
    logging.debug("save_bad_file_db: src_info = %s", src_info)
    data = {}
    if src_info is not None:
        copyvals = ["filename", "dataset_type", "filesize", "chksum", "chksum_type"]
        for val in copyvals:
            data[val] = src_info[val]
        data["file_registration_process_id"] = src_info["process_id"]

    copyvals = ["uniq_filename", "relpath", "disk_usage", "rejected_msg", "rejected_date"]
    for val in copyvals:
        data[val] = bad_info[val]
    logging.debug("bad file db row info = %s", data)

    cols = data.keys()
    sql = "insert into dbb_bad_file (%s) values (:%s)" % (",".join(cols), ",:".join(cols))
    curs = dbh.cursor()
    logging.debug("sql = %s", sql)
    curs.execute(sql, data)
    dbh.commit()


def get_registration_process_id(dbh, uuid):
    """Save to the database base information used for provenance

    Parameters
    ----------
    dbh : `cx_Oracle.Connection`
        Open database connection with write access to DBB tables
    uuid : `str`
        ID currently used to group the saving of multiple files into single
        process for provenance

    Returns
    -------
    process_id : `int` or None
        Process id to be used for provenance
    """
    curs = dbh.cursor()

    process_id = None
    sql = "select file_registration_process_id from file_registration_lookup where uuid=:uuid"
    logging.debug("sql = %s", sql)
    curs.execute(sql, {"uuid": uuid})
    row = curs.fetchone()
    if row is not None:
        process_id = row[0]

    return process_id


def create_new_process(dbh, info):
    """Creates a row in the process table for this file registration

    Parameters
    ----------
    dbh : `cx_Oracle.Connection`
        Open database connection with write access to DBB tables
    info : `dict`
        Information about process that staged files to DBB gateway
        Required: exec_name, exec_host, timestamp (epoch)

    Returns
    -------
    The newly created process id to be used in provenance
    """
    process_id = get_sequence_val(dbh, "PROCESS_SEQ")

    curs = dbh.cursor()
    sql = ("insert into process (ID, NAME, INFO_TABLE, EXEC_HOST, START_TIME, ROOT_PROCESS_ID) "
           "values (:id, :name, :info_table, :exec_host, :start_time, :id)")
    logging.debug("sql = %s", sql)
    curs.execute(sql, {"id": process_id,
                       "name": info["exec_name"],
                       "exec_host": info["exec_host"],
                       "info_table": "file_registration",
                       "start_time": datetime.fromtimestamp(info["timestamp"])})

    return process_id


def save_registration_info(dbh, src_info):
    """Save to the database base information used for provenance

    Parameters
    ----------
    dbh : `cx_Oracle.Connection`
        Open database connection with write access to DBB tables
    src_info : `dict`
        dictionary containing information specific to this ingestion

    Returns
    -------
    The newly created process id to be used in provenance
    """
    process_id = create_new_process(dbh, src_info)

    curs = dbh.cursor()

    sql = ("insert into file_registration (PROCESS_ID, USERNAME, PROV_MSG) values "
           "(:process_id, :username, :provmsg)")
    logging.debug("sql = %s", sql)
    curs.execute(sql, {"process_id": process_id,
                       "username": src_info["user"],
                       "provmsg": src_info["prov_msg"]})

    sql = ("insert into file_registration_lookup (UUID, FILE_REGISTRATION_PROCESS_ID) values "
           "(:uuid, :process_id)")
    logging.debug("sql = %s", sql)
    curs.execute(sql, {"uuid": src_info["uuid"],
                       "process_id": process_id})

    dbh.commit()

    return process_id
