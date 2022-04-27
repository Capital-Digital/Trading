from django import template

register = template.Library()


def drop_(value):
    return value.replace('_', ' ')