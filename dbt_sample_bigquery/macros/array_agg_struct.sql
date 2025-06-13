{% macro array_agg_struct(fields, order_by=none, limit=none) %}
  {%- set field_list = [] -%}
  {%- for field in fields -%}
    {%- if ':' in field -%}
      {%- set field_parts = field.split(':') -%}
      {%- set field_name = field_parts[0].strip() -%}
      {%- set field_alias = field_parts[1].strip() -%}
      {%- set _ = field_list.append("'" + field_alias + "', " + field_name) -%}
    {%- else -%}
      {%- set _ = field_list.append("'" + field + "', " + field) -%}
    {%- endif -%}
  {%- endfor -%}
  
  array_agg(object_construct({{ field_list | join(', ') }})
  {%- if order_by %} within group (order by {{ order_by }}){% endif -%}
  )
  {%- if limit %} over (rows between unbounded preceding and {{ limit - 1 }} following){% endif -%}
{% endmacro %}