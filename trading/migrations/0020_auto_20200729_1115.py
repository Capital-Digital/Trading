# Generated by Django 3.0.6 on 2020-07-29 11:15

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('trading', '0019_auto_20200728_1512'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='account',
            name='instrument',
        ),
        migrations.RemoveField(
            model_name='account',
            name='settlement',
        ),
    ]
