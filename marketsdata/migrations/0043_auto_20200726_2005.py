# Generated by Django 3.0.6 on 2020-07-26 20:05

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('marketsdata', '0042_remove_market_updated'),
    ]

    operations = [
        migrations.AlterField(
            model_name='exchange',
            name='status',
            field=models.CharField(blank=True, choices=[('ok', 'ok'), ('maintenance', 'maintenance'), ('shutdown', 'shutdown'), ('error', 'error')], default='ok', max_length=12, null=True),
        ),
    ]
