# Generated by Django 3.0.6 on 2021-03-06 21:29

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('marketsdata', '0119_remove_market_percentage'),
    ]

    operations = [
        migrations.RenameField(
            model_name='market',
            old_name='listing',
            new_name='listing_date',
        ),
    ]
