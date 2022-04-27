from django import template

register = template.Library()


@register.filter(name='drop_')
def drop_(value):
    return value.replace('_', ' ')


