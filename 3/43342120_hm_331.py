from jinja2 import Template


x = int(input())
t = Template("{% for i in range(2, end, 2) %}{{ i }} {% endfor %}")
print(t.render(end=x))
