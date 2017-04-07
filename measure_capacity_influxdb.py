#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""measure_capacity_influxdb.py

Cliente para testear InfluxDB

Usage:
  measure_capacity_influxdb.py [options] <command> [<args>...]

Commands:
  shards                        Analiza el consumo de disco de los shards

General options:
  -h --help                       show help message and exit
  -V --version                    show version and exit
  -v --verbose                    print status messages
  -d --debug                      print debug messages
  --influx_host <ifhost>          InfluxDB host [default: 127.0.0.1]
  --influx_port <ifport>          InfluxDB port [default: 8086]
  --influx_user <ifuser>          InfluxDB user [default: admin]
  --influx_password <ifpass>      InfluxDB password
  --influx_db <ifdb>              InfluxDB data base, apply to all dbs if not defined
  --influx_timeout <iftimeout>    InfluxDB timeout
  --influx_batch <batch_size>     InfluxDB batch size writing points [default: 300]
  --influx_ssh_host <ifhost>      SSH InfluxDB host, por defecto el mismo de --influx_host
  --influx_ssh_port <ifport>      SSH InfluxDB port [default: 22]
  --influx_ssh_user <ifuser>      SSH InfluxDB user
  --influx_ssh_password <ifpass>  SSH InfluxDB password

See measure_capacity_influxdb.py <command> --help for more information on a specific command.
"""

import sys
try:
    from docopt import docopt, DocoptExit, DocoptLanguageError
except ImportError:
    print("docopt module has to be installed")
    sys.exit(1)

from influxdb import InfluxDBClient
from datetime import datetime
import math
import re
import paramiko
import select
import random
import string

import logging

FORMAT = "[%(levelname)s %(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(level=logging.ERROR, format=FORMAT)
logger = logging.getLogger(__name__)


__version__ = '0.1'
RETURN_CODE_ERROR = 1


################################################################################
class Client(object):
    """
    InfluxDB client to make testing
    """

    def __init__(self, args):
        logger.debug("Inicializar cliente InfluxDB.\n" \
                "Host: %s:%s/%s\n" \
                "User: %s\n" \
                "Pass: %s\n" \
                "Timeout: %s\n" % (args['--influx_host'],
                    args['--influx_port'],
                    args['--influx_db'],
                    args['--influx_user'],
                    args['--influx_password'],
                    args['--influx_timeout']))

        self.client = InfluxDBClient(
                args['--influx_host'],args['--influx_port'],
                args['--influx_user'], args['--influx_password'],
                args['--influx_db'], timeout = args['--influx_timeout'])

        self.influx_db = args['--influx_db']

        # SSH params
        if args['--influx_ssh_host']:
            self.ssh_host = args['--influx_ssh_host']
        else:
            self.ssh_host = args['--influx_host']
        self.ssh_user = args['--influx_ssh_user']
        self.ssh_pass = args['--influx_ssh_password']

        # Dict para almacenar los tamaños de los shards obtenidos por ssh
        self.shards_size_list = {}

    def _parse_du(self, line):
        """
        Parseamos una linea tipo:
            23248   /opt/influxdb/data/ddbb/default/1939
        El id sera el ultimo elemento tras la /
        El primer elemento seran los bytes
        El numero de espacios en blanco pueda que sea variable
        Retornamos id del shard y su tamaño
        """
        regexp = re.compile(r"([0-9]+)\s+.*/([0-9]+)")
        r = re.match(regexp, line)
        return int(r.group(2)), int(r.group(1))

    def _get_all_shards_size(self, var_dir):
        """
        Conecta por ssh y ejecuta un comando para obtener el tamaño de todos los shards
        """

        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.ssh_host, username=self.ssh_user, password=self.ssh_pass)
            logger.debug("Conectado por ssh")
        except paramiko.AuthenticationException:
            logger.error("Authentication failed when connecting ssh")
            sys.exit(1)
        except Exception as ex:
            logger.error("Excepcion conectando por ssh: %s", ex)
            sys.exit(1)

        cmd = "sudo find %s -maxdepth 3 -mindepth 3 -exec du -b {} \;" % var_dir
        logger.debug("Enviando comando: %s", cmd)
        (stdin, stdout, stderr) = ssh.exec_command(cmd, get_pty=True)

        for line in stdout.readlines():
            logger.debug(line)
            shard_id, size_bytes = self._parse_du(line)
            self.shards_size_list[shard_id] = size_bytes

        logger.debug("shards_size_list: %s", self.shards_size_list)

        for line in stderr.readlines():
            logger.error("SSH COMMAND stderr: %s", line)


    def _get_shard_size(self, shard_id):
        """
        De la lista ya bajada, obtenemos el que nos solicitan
        El shard id es unico para todo influx
        """
        return self.shards_size_list.get(shard_id)

    def _size(self, size_bytes):
        if (size_bytes == 0):
            return '0B'
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes/p, 2)
        return '%s %s' % (s, size_name[i])

    ############################################################################
    def shards(self, args):
        """
Obtener el consumo de disco por parte de los shards, pudiendo filtrar por fecha

Usage:
  measure_capacity_influxdb.py shards [-F=<YYYYMMDD>] [-T=<YYYYMMDD>] [-D=<influxdb_var>] [--full]

Options:
  -F,--from=<YYYYMMDD>                  Calcular a partir de esta fecha [default: 20000101]
  -T,--to=<YYYYMMDD>                    Calcular hasta esta fecha [default: 20990101]
  -D,--dir=<influxdb_var>               Directorio de almacenamiento [default: /var/lib/influxdb]
  --full                                Mostrar los datos de todas las DBs
        """

        from_date = datetime.strptime(args['--from'],"%Y%m%d")
        to_date = datetime.strptime(args['--to'],"%Y%m%d")
        var_dir = args['--dir']
        logger.info("Calculando tamaño de shards desde %s hasta %s para database %s (dir: %s)"
                    , from_date, to_date, self.influx_db, var_dir)

        logger.info("Conectando por ssh para obtener tamaño de todos los shards")
        self._get_all_shards_size(var_dir)

        logger.debug("Ejecutando query SHOW SHARDS para obtener la info de fecha de cada uno")
        shards = self.client.query("SHOW SHARDS")
        total_size = 0

        logger.info("Analizando cada grupo de shards para calcula su tamaño total")
        for group in shards:
            # Ignoramos arrays vacios
            try:
                database = group[0].get("database")
            except Exception:
                pass

            # Si esta definida la db, solo analizamos los shards de esa db
            if self.influx_db and database != self.influx_db:
                continue

            # Iniciamos el array de la database
            shard_size = 0

            for s in group:
                shard_start_time = datetime.strptime(s.get("start_time"),"%Y-%m-%dT%H:%M:%SZ")
                shard_end_time = datetime.strptime(s.get("end_time"),"%Y-%m-%dT%H:%M:%SZ")

                # Si el shard es mas antiguo que la fecha que queremos, lo descartamos
                # Si es mas nuevo que lo que queremos lo descartamos tambien
                if shard_start_time < from_date:
                    continue
                if shard_end_time > to_date:
                    continue

                try:
                    shard_size += self._get_shard_size(s.get("id"))
                except Exception as ex:
                    logger.debug("Shard no existente en el FS %s", s.get("id"))

            if args['--full'] or self.influx_db:
                print("%s: %s" % (self._size(shard_size), database))
            total_size += shard_size

        if not self.influx_db:
            print("TOTAL: %s" % (self._size(total_size)))


    ############################################################################

################################################################################
#
def _execute_cmd(method, args):
    """
    Execute command
    """
    method(args)


def main():
    """
    Create a client, parse the arguments received on the command line and call
    the appropriate method.
    """
    try:
        args = docopt(__doc__, version=__version__, options_first=True)
    except DocoptExit as e:
        sys.stderr.write("ERROR: invalid parameters\n\n%s" % e)
        sys.exit(RETURN_CODE_ERROR)


    # set logging
    if args['--verbose']:
        logger.setLevel(logging.INFO)
    if args['--debug']:
        logger.setLevel(logging.DEBUG)

    cli = Client(args)

    cmd = args['<command>']

    # test if method exists
    if hasattr(cli, cmd):
        method = getattr(cli, cmd)
    else:
        sys.stderr.write("This command '%s' doesn't exist, try:\n%s --help" % (cmd, sys.argv[0]))
        sys.exit(RETURN_CODE_ERROR)

    # re-parse docopt with the relevant docstring from name of cmd
    docstring = method.__doc__.strip()
    if 'Usage:' in docstring:
        try:
            args.update(docopt(docstring, argv=sys.argv[sys.argv.index(cmd):]))
        except DocoptLanguageError as e:
            sys.stderr.write("ERROR: %s\n\n\n%s: %s\n" % (e.message, cmd, docstring))
            sys.exit(RETURN_CODE_ERROR)

    # execute the command
    _execute_cmd(method, args)

if __name__ == '__main__':
    main()
    sys.exit(0)
