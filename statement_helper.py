from ipaddress import ip_address

from sqlalchemy.sql import or_, and_

from base64_url import base64_url_decode

def sort_statement(
		statement,
		table,
		attempted_column,
		order,
		default_column,
		default_descending,
		additional_columns=None,
	):
	try:
		sort_column = getattr(table.c, attempted_column)
	except:
		attempted_column = default_column
		if isinstance(default_column, str):
			sort_column = getattr(table.c, default_column)
		else:
			sort_column = default_column

	if default_descending:
		if 'asc' != order:
			order = 'desc'
	else:
		if 'desc' != order:
			order = 'asc'

	column_order = getattr(sort_column, order)
	statement = statement.order_by(column_order())
	if additional_columns:
		if not isinstance(additional_columns, list):
			additional_columns = [additional_columns]
		for additional_column in additional_columns:
			sort_column = getattr(table.c, additional_column)
			statement = statement.order_by(getattr(sort_column, order)())
	return statement

def paginate_statement(statement, page=0, perpage=0):
	if 0 < page:
		statement = statement.offset(page * perpage)
	if perpage:
		statement = statement.limit(perpage)
	return statement

def id_filter(filter, filter_field, column):
	conditions = []
	if filter_field in filter:
		if list is not type(filter[filter_field]):
			filter[filter_field] = [filter[filter_field]]
		block_conditions = []
		for id in filter[filter_field]:
			if not isinstance(id, bytes):
				try:
					id = base64_url_decode(id)
				except:
					continue
			block_conditions.append(column == id)
		if block_conditions:
			conditions.append(or_(*block_conditions))
		else:
			conditions.append(False)
	return conditions

def int_cutoff_filter(
		filter,
		filter_field_less_than,
		filter_field_greater_than,
		column,
	):
	conditions = []
	if filter_field_less_than in filter:
		try:
			less_than = int(filter[filter_field_less_than])
		except:
			conditions.append(False)
		else:
			conditions.append(
				column < less_than
			)
	if filter_field_greater_than in filter:
		try:
			greater_than = int(filter[filter_field_greater_than])
		except:
			conditions.append(False)
		else:
			conditions.append(
				column > greater_than
			)
	return conditions

def time_cutoff_filter(filter, filter_field, column):
	return int_cutoff_filter(
		filter,
		filter_field + '_before',
		filter_field + '_after',
		column,
	)

def string_equal_filter(filter, filter_field, column):
	conditions = []
	if filter_field in filter:
		if list is not type(filter[filter_field]):
			filter[filter_field] = [filter[filter_field]]
		block_conditions = []
		for string_equal in filter[filter_field]:
			block_conditions.append(
				column == str(string_equal)
			)
		if block_conditions:
			conditions.append(or_(*block_conditions))
		else:
			conditions.append(False)
	return conditions

def string_not_equal_filter(filter, filter_field, column):
	conditions = []
	if filter_field in filter:
		if list is not type(filter[filter_field]):
			filter[filter_field] = [filter[filter_field]]
		block_conditions = []
		for string_equal in filter[filter_field]:
			block_conditions.append(
				column != str(string_equal)
			)
		if block_conditions:
			conditions.append(or_(*block_conditions))
		else:
			conditions.append(False)
	return conditions

def string_like_filter(filter, filter_field, column):
	conditions = []
	if filter_field in filter:
		if list is not type(filter[filter_field]):
			filter[filter_field] = [filter[filter_field]]
		block_conditions = []
		for string_like in filter[filter_field]:
			block_conditions.append(
				column.like(str(string_like), escape='\\')
			)
		if block_conditions:
			conditions.append(or_(*block_conditions))
		else:
			conditions.append(False)
	return conditions

def bitwise_filter(filter, filter_field, column):
	conditions = []
	if 'with_' + filter_field in filter:
		bits = filter['with_' + filter_field]
		if not isinstance(bits, int):
			try:
				bits = int.from_bytes(bits, 'big')
			except:
				return[False]
		conditions.append(
			and_(column.op('&')(bits) == bits).self_group()
		)
	if 'without_' + filter_field in filter:
		bits = filter['without_' + filter_field]
		if isinstance(bits, bytes):
			bits = int.from_bytes(bits, 'big')
		elif not isinstance(bits, int):
			return conditions
		conditions.append(
			and_(column.op('&')(bits) != bits).self_group()
		)
	return conditions

def remote_origin_filter(filter, filter_field, column):
	conditions = []
	if 'with_' + filter_field in filter:
		if list is not type(filter['with_' + filter_field]):
			filter['with_' + filter_field] = [
				filter['with_' + filter_field],
			]
		block_conditions = []
		for remote_origin in filter['with_' + filter_field]:
			try:
				remote_origin = ip_address(str(remote_origin))
			except:
				pass
			else:
				block_conditions.append(
					column == remote_origin.packed
				)
		if block_conditions:
			conditions.append(or_(*block_conditions))
		else:
			conditions.append(False)
	if 'without_' + filter_field in filter:
		if list is not type(filter['without_' + filter_field]):
			filter['without_' + filter_field] = [
				filter['without_' + filter_field],
			]
		block_conditions = []
		for remote_origin in filter['without_' + filter_field]:
			try:
				remote_origin = ip_address(str(remote_origin))
			except:
				pass
			else:
				block_conditions.append(
					column != remote_origin.packed
				)
		if block_conditions:
			conditions.append(or_(*block_conditions))
		else:
			conditions.append(False)
	return conditions
