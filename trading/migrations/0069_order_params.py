# Generated by Django 3.0.6 on 2020-10-23 14:46

import django.contrib.postgres.fields.jsonb
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('trading', '0068_remove_order_order_type'),
    ]

    operations = [
        migrations.AddField(
            model_name='order',
            name='params',
            field=django.contrib.postgres.fields.jsonb.JSONField(null=True),
        ),
    ]
