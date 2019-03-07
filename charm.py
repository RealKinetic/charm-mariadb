from charmhelpers.core import host

from juju import charm, model, unit, endpoint
from juju.errors import ConfigError

import mariadb


@model.configure
def configure_workload():
    """
    This is called when the charm needs to be configured in some manner.
    This will run before any workload has been started.
    """
    root_password = model.config.get('root-password')

    if not root_password:
        root_password = host.pwgen(32)

    model.config.set('root-password', root_password)


@model.prepare_workload
def prepare_workload(workload):
    """
    This handler is called when the charm has been configured and is ready
    to deploy workloads.

    This should potentially work for both the machine and k8s world?
    Assuming that the VM is running Docker/LXD.
    """
    root_password = model.config.get('root-password')

    if not root_password:
        raise ConfigError("Missing db root password (charm not configured?)")

    workload.set_oci_image(charm.resources['mariadb'])
    workload.open_port('db', containerPort=3306, protocol='TCP')
    workload.env.set('MYSQL_ROOT_PASSWORD', root_password)

    # TODO: who/what is dealing with storage?


@endpoint.join('database')
def handle_join(db, request):
    """
    Called when a request for a new database endpoint is made.

    If a database does not exists for this request, one will be created.

    @param db: The `database` endpoint.
    @param request: The request being made to connect to this db.
    """
    creds = model.data.get('credentials', {})

    context = creds.get(request.application_name, None)

    if not context:
        context = creds[request.application_name] = {
            'username': host.pwgen(20),
            'password': host.pwgen(20),
            'database': request.database_name or request.application_name
        }

    username = context['username']
    password = context['password']
    db_name = context['database']

    with get_admin_connection() as connection:
        with mariadb.cursor(connection) as cursor:
            mariadb.create_database(cursor, db_name)
            mariadb.ensure_grant(
                cursor,
                db_name,
                username,
                password,
                # TODO: think about this from a ephemeral pod perspective
                request.address
            )

    request.set_state(context)
    request.ack()


@endpoint.leave('database')
def handle_leave(db_endpoint, request):
    """
    Called when a request to leave/drop an endpoint is made.

    @param db: The `database` endpoint.
    @param request: The request being made to abandon this db.
    """
    with get_admin_connection() as connection:
        with mariadb.cursor(connection) as cursor:
            mariadb.cleanup_grant(
                cursor,
                request.username,
                request.address,
            )

    request.ack()


def get_admin_connection():
    """
    Returns a `mysql.connector.Connection` object that has 'root' privileges
    over the underlying mariadb.

    Use with care.
    """
    root_password = model.config.get('root-password')

    if not root_password:
        raise ConfigError("Missing root password (charm not configured yet?)")

    return mariadb.get_connection(
        host=unit.host_name,
        password=root_password,
        user='root',
    )
