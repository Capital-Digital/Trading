# Generated by Django 3.0.6 on 2021-05-27 14:08

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('trading', '0111_order_options'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='order',
            name='options',
        ),
    ]
