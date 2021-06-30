# Generated by Django 3.0.6 on 2021-03-16 16:21

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('marketsdata', '0146_remove_exchange_dollar_currency'),
    ]

    operations = [
        migrations.AddField(
            model_name='currency',
            name='dollar_currency',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='dollar_currency', to='marketsdata.Exchange'),
        ),
    ]
