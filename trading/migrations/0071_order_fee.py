# Generated by Django 3.0.6 on 2020-10-24 10:49

import django.contrib.postgres.fields.jsonb
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('trading', '0070_remove_order_fee'),
    ]

    operations = [
        migrations.AddField(
            model_name='order',
            name='fee',
            field=django.contrib.postgres.fields.jsonb.JSONField(null=True),
        ),
    ]
