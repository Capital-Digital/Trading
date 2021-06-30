# Generated by Django 3.0.6 on 2021-02-28 08:44

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('marketsdata', '0075_market_config'),
    ]

    operations = [
        migrations.AddField(
            model_name='currency',
            name='quotes',
            field=models.ManyToManyField(related_name='market_quotes', to='marketsdata.Exchange'),
        ),
    ]
