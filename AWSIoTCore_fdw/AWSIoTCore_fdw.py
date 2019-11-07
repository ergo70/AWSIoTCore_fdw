import boto3
from botocore.exceptions import ClientError
from json import dumps
from multicorn import ForeignDataWrapper, TableDefinition, ColumnDefinition
from multicorn.utils import log_to_postgres, WARNING, ERROR


class AIC_fdw(ForeignDataWrapper):
    """A foreign data wrapper for accessing the AWS IoT Core.

    Valid options:
        - url : the IoT core URL
        - region : the AWS region
        - aws_access_key : AWS access key
        - aws_secret_key : AWS secret key
        - table_type : the type of the foreign table
    """

    _startup_cost = 1000

    @classmethod
    def import_schema(cls, schema, srv_options, options, restriction_type, restricts):

        return [TableDefinition('{}_things'.format(schema), schema=schema, options={'table_type': 'thing'}, columns=[
            ColumnDefinition('thing_name',  type_name='text'),
            ColumnDefinition('thing_type_name',  type_name='text'),
            ColumnDefinition('thing_arn',  type_name='text'),
            ColumnDefinition('thing_version',  type_name='integer'),
            ColumnDefinition('thing_groups', type_name='jsonb'),
            ColumnDefinition('thing_attributes', type_name='jsonb'),
            ColumnDefinition('thing_shadow_data', type_name='jsonb')
        ]),
            TableDefinition('{}_thing_types'.format(schema), schema=schema, options={'table_type': 'thing-type'}, columns=[
                ColumnDefinition('thing_type_name',  type_name='text'),
                ColumnDefinition('thing_type_arn',  type_name='text'),
                ColumnDefinition('thing_type_properties',
                                 type_name='jsonb'),
                ColumnDefinition('thing_type_metadata', type_name='jsonb')
            ]),
            TableDefinition('{}_thing_groups'.format(schema), schema=schema, options={'table_type': 'thing-group'}, columns=[
                ColumnDefinition('thing_group_name',  type_name='text'),
                ColumnDefinition('thing_group_arn',  type_name='text')
            ])]

    def __init__(self, fdw_options, fdw_columns):
        super(AIC_fdw, self).__init__(fdw_options, fdw_columns)

        self.MAX_RESULTS = 250

        self.LIKE_WILDCARD = '%'

        self.table_type = fdw_options.get('table_type', 'thing')

        self.region = fdw_options.get("region")
        if self.region is None:
            log_to_postgres("Please set the AWS region", ERROR)

        self.aws_access_key = fdw_options.get('aws_access_key')
        if self.aws_access_key is None:
            log_to_postgres("Please set the AWS access key", ERROR)

        self.aws_secret_key = fdw_options.get('aws_secret_key')
        if self.aws_secret_key is None:
            log_to_postgres("Please set the AWS secret key", ERROR)

        self.endpoint_url = fdw_options.get("url")

        self.columns = fdw_columns

        # self._row_id_column = fdw_columns.keys()[0]
        if 'thing' == self.table_type.lower():
            self._row_id_column = 'thing_name'
        elif 'thing-type' == self.table_type.lower():
            self._row_id_column = 'thing_type_name'
        elif 'thing-group' == self.table_type.lower():
            self._row_id_column = 'thing_group_name'
        else:
            log_to_postgres("This FDW does not support table_type {}".format(
                self.table_type), ERROR)

        client_args = {'aws_access_key_id': self.aws_access_key,
                       'aws_secret_access_key': self.aws_secret_key, 'region_name': self.region}

        if self.endpoint_url:
            client_args.update({'endpoint_url': self.endpoint_url})

        self.core_client = boto3.client('iot', **client_args)

        self.data_client = boto3.client('iot-data', **client_args)

    def get_path_keys(self):
        return[((self._row_id_column,), 1)]

    def execute(self, quals, columns):
        if 'thing' == self.table_type.lower():
            for row in self._execute_thing(quals, columns):
                yield row
        elif 'thing-type' == self.table_type.lower():
            for row in self._execute_thing_type(quals, columns):
                yield row
        elif 'thing-group' == self.table_type.lower():
            for row in self._execute_thing_group(quals, columns):
                yield row

    def _execute_thing_group(self, quals, columns):
        thing_group_prefix = None

        if quals:
            for qual in quals:
                if qual.field_name.lower() == 'thing_group_name':
                    if qual.operator == '=':
                        thing_group_prefix = qual.value
                    elif qual.operator == '~~' and qual.field_name.count(self.LIKE_WILDCARD) == 1 and qual.field_name.endswith(self.LIKE_WILDCARD):
                        thing_group_prefix = qual.value[:-1]

        try:
            nextToken = None

            while True:
                call_args = {'maxResults': self.MAX_RESULTS}

                if nextToken:
                    call_args.update({'nextToken': nextToken})

                if thing_group_prefix:
                    call_args.update({'namePrefixFilter': thing_group_prefix})

                response = self.core_client.list_thing_groups(**call_args)

                nextToken = response.get('nextToken')

                for thing_type in response.get('thingGroups'):
                    # Always available, since this is the primary key column
                    thing_type_name = thing_type.get('groupName')
                    row = {self._row_id_column: thing_type_name}

                    if 'thing_group_arn' in columns:
                        row.update(
                            {'thing_group_arn': thing_type.get('groupArn')})

                    yield row

                if not nextToken:
                    break

        except ClientError as ce:
            log_to_postgres("No thing groups from IoT core with ClientError {}".format(
                ce), ERROR)
        except Exception as e:
            log_to_postgres("Unknown error {}".format(e), ERROR)

    def _execute_thing_type(self, quals, columns):
        thing_type_name = None

        if quals:
            for qual in quals:
                if qual.field_name.lower() == 'thing_type_name':
                    if qual.operator == '=':
                        thing_type_name = qual.value

        try:
            nextToken = None

            while True:
                call_args = {'maxResults': self.MAX_RESULTS}

                if nextToken:
                    call_args.update({'nextToken': nextToken})

                if thing_type_name:
                    call_args.update({'thingTypeName': thing_type_name})

                response = self.core_client.list_thing_types(**call_args)

                nextToken = response.get('nextToken')

                for thing_type in response.get('thingTypes'):
                    # Always available, since this is the primary key column
                    thing_type_name = thing_type.get('thingTypeName')
                    row = {self._row_id_column: thing_type_name}

                    if 'thing_type_arn' in columns:
                        row.update(
                            {'thing_type_arn': thing_type.get('thingTypeArn')})
                    if 'thing_type_properties' in columns:
                        row.update(
                            {'thing_type_properties': dumps(thing_type.get('thingTypeProperties'))})
                    if 'thing_type_metadata' in columns:
                        row.update(
                            {'thing_type_metadata': dumps(thing_type.get('thingTypeMetadata'), default=str)})

                    yield row

                if not nextToken:
                    break

        except ClientError as ce:
            log_to_postgres("No thing types from IoT core with ClientError {}".format(
                ce), ERROR)
        except Exception as e:
            log_to_postgres("Unknown error {}".format(e), ERROR)

    def _execute_thing(self, quals, columns):
        thing_type_name = None

        if quals:
            for qual in quals:
                if qual.field_name.lower() == 'thing_type_name':
                    if qual.operator == '=':
                        thing_type_name = qual.value

        try:
            nextToken = None

            while True:

                call_args = {'maxResults': self.MAX_RESULTS}

                if nextToken:
                    call_args.update({'nextToken': nextToken})

                if thing_type_name:
                    call_args.update({'thingTypeName': thing_type_name})

                response = self.core_client.list_things(**call_args)

                nextToken = response.get('nextToken')

                for thing in response.get('things'):
                    # Always available, since this is the primary key column
                    thing_name = thing.get('thingName')
                    row = {self._row_id_column: thing_name}

                    if 'thing_groups' in columns:
                        row.update(
                            {'thing_groups': self._thing_groups_for_thing(thing_name)})
                    if 'thing_type_name' in columns:
                        row.update(
                            {'thing_type_name': thing.get('thingTypeName')})
                    if 'thing_arn' in columns:
                        row.update(
                            {'thing_arn': thing.get('thingArn')})
                    if 'thing_version' in columns:
                        row.update(
                            {'thing_version': thing.get('version')})
                    if 'thing_attributes' in columns:
                        row.update(
                            {'thing_attributes': dumps(thing.get('attributes'))})
                    if 'thing_shadow_data' in columns:
                        row.update(
                            {'thing_shadow_data': self._thing_shadow_data(thing_name)})

                    yield row

                if not nextToken:
                    break

        except ClientError as ce:
            log_to_postgres("No things from IoT core with ClientError {}".format(
                ce), ERROR)
        except Exception as e:
            log_to_postgres("Unknown error {}".format(e), ERROR)

    def _thing_groups_for_thing(self, thing):
        try:
            response = self.core_client.list_thing_groups_for_thing(
                thingName=thing, maxResults=self.MAX_RESULTS)
            groups = response.get('thingGroups')
            nextToken = response.get('nextToken')

            while nextToken:
                response = self.core_client.list_thing_groups_for_thing(
                    thingName=thing, maxResults=self.MAX_RESULTS, nextToken=nextToken)
                groups.extend(response.get('thingGroups'))
                nextToken = response.get('nextToken')

            return dumps(groups)

        except ClientError as ce:
            log_to_postgres(
                "No thing groups from IoT core for thing {} with ClientError {}".format(thing, ce), WARNING)
        except Exception as e:
            log_to_postgres("Unknown error {}".format(e), ERROR)

        return None

    def _thing_shadow_data(self, thing):
        data = None
        try:
            shadow = self.data_client.get_thing_shadow(thingName=thing)
            payload = shadow.get("payload")
            if payload:
                data = payload.read()
        except ClientError as ce:
            log_to_postgres(
                "No data in IoT shadow for thing {} with ClientError {}".format(thing, ce), WARNING)
        except Exception as e:
            log_to_postgres("Unknown error {}".format(e), ERROR)

        return data

    @property
    def rowid_column(self):
        return self._row_id_column

    def insert(self, values):
        if 'thing' == self.table_type.lower():
            return self._insert_thing(values)
        elif 'thing-type' == self.table_type.lower():
            return self._insert_thing_type(values)

    def _insert_thing_type(self, values):
        raise NotImplementedError

    def _insert_thing(self, values):
        new_thing = values.get('thing_name')
        new_thing_type = values.get('thing_type')
        try:
            if new_thing_type:
                self.core_client.create_thing(
                    thingName=new_thing,
                    thingTypeName=new_thing_type
                )
            else:
                self.core_client.create_thing(
                    thingName=new_thing
                )
        except ClientError as ce:
            log_to_postgres(
                "Insert failed in IoT shadow for thing {} with ClientError {}".format(new_thing, ce), ERROR)
        except Exception as e:
            log_to_postgres("Unknown error {}".format(e), ERROR)

        return values

    def update(self, rowid, newvalues):
        if 'thing' == self.table_type.lower():
            return self._update_thing(rowid, newvalues)
        elif 'thing-type' == self.table_type.lower():
            return self._update_thing_type(rowid, newvalues)

    def _update_thing_type(self, rowid, newvalues):
        raise NotImplementedError

    def _update_thing(self, rowid, newvalues):
        if 'thing_shadow_data' in newvalues.keys():
            try:
                shadow = self.data_client.update_thing_shadow(
                    thingName=rowid,
                    payload=newvalues.get('thing_shadow_data')
                )
                payload = shadow.get("payload")
                if payload:
                    newvalues['thing_shadow_data'] = payload.read()
            except ClientError as ce:
                log_to_postgres(
                    "Data update failed in IoT shadow for thing {} with ClientError {}".format(rowid, ce), ERROR)
            except Exception as e:
                log_to_postgres("Unknown error {}".format(e), ERROR)

        return newvalues

    def delete(self, rowid):
        if 'thing' == self.table_type.lower():
            return self._delete_thing(rowid)
        elif 'thing-type' == self.table_type.lower():
            return self._delete_thing_type(rowid)

    def _delete_thing_type(self, rowid):
        raise NotImplementedError

    def _delete_thing(self, rowid):
        try:
            self.core_client.delete_thing(
                thingName=rowid
            )
        except ClientError as ce:
            log_to_postgres(
                "Delete failed in IoT shadow for thing {} with ClientError {}".format(rowid, ce), ERROR)
        except Exception as e:
            log_to_postgres("Unknown error {}".format(e), ERROR)

        return None

