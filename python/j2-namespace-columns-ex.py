# You can use the following in a python console/workspace to see examples of:
#  - jinja namespace() (reference a variable in the outer scope (loop))
#  - passing a value into a template (use a dictionary with each entry = "variable_name": variable_value
#  - slice() jinja filter (can be used to split a list into columns
#
# You can also just run this .py file.
#
# @since 2018-10-23
# @author Ashley Engelund ashley.engelund@gmail.com < weedySeaDragon @ github >

# https://code.visualstudio.com/docs/python/python-tutorial
# Run  : & c:/Users/chuong.nguyenh2/Documents/dbt_oracle_d5/.dbtenv/Scripts/python.exe c:/Users/chuong.nguyenh2/Documents/dbt_oracle_d5/python/j2-namespace-columns-ex.py

import jinja2
from jinja2 import Template

entries = ['a', 'b', 'c', 'd', 'e', 'f', 'g']

templ_content = ("""{% for column in entries | slice(2)  %}
    loop.index: {{ loop.index }}
    loop.length = {{ loop.length }}
    {% set colsIndex = namespace(loopindex = loop.index) %}
     column {{ loop.index }}:
    {% for entry in column %}
        loop.index = {{ loop.index }} for entry: {{ entry }}
           loop.length = {{ loop.length }}
           outer loop.index = {{ colsIndex.loopindex }}
    {% endfor %}
{% endfor %}""")

print(Template(templ_content).render())
print("--------------------------------------------------")
print(Template(templ_content).render({"entries": entries}))
