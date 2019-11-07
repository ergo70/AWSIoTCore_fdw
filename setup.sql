create extension multicorn;

create server aws_iot_core_vcf foreign data wrapper multicorn options (wrapper 'AWSIoTCore_fdw.AWSIoTCore_fdw.AIC_fdw',
region '',
aws_access_key '',
aws_secret_key '');

import foreign schema aws
from
server aws_iot_core_vcf
into
	aws;

select
	*
from
	aws.aws_things;

select
	*
from
	aws.aws_thing_groups;

select
	*
from
	aws.aws_thing_types;
