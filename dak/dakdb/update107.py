#!/usr/bin/env python

"""
Adds bin_dep11 table. Stores appstream metadata per binary
"""

# Copyright (C) 2014 Abhishek Bhattacharjee <abhishek.bhattacharjee11@gmail.com>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

############################################################################

# the script is part of project under Google Summer of Code '14
# Project: AppStream/DEP-11 for the Debian Archive
# Mentor: Matthias Klumpp

############################################################################


import psycopg2
from daklib.dak_exceptions import DBUpdateError
from daklib.config import Config
from daklib.dbconn import *

statements = [
    """
    CREATE TABLE bin_dep11(id SERIAL PRIMARY KEY,
    binary_id integer not null,
    metadata text not null,
    hints text,
    ignore boolean not null
    );
    """,

    """
    ALTER TABLE bin_dep11 ADD CONSTRAINT binaries_bin_dep11
    FOREIGN KEY (binary_id) REFERENCES binaries (id) ON DELETE CASCADE;
    """
]

##############################################################################

def do_update(self):
    print __doc__
    try:
        c = self.db.cursor()
        for stmt in statements:
            c.execute(stmt)

        c.execute("UPDATE config SET value = '107' WHERE name = 'db_revision'")
        self.db.commit()

    except psycopg2.ProgrammingError as msg:
        self.db.rollback()
        raise DBUpdateError("Unable to apply sick update 107, rollback issued. Error message: {0}".format(msg))
