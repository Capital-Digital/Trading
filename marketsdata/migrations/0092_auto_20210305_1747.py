# Generated by Django 3.0.6 on 2021-03-05 17:47

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('marketsdata', '0091_auto_20210305_1745'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='market',
            name='type',
        ),
        migrations.AddField(
            model_name='market',
            name='type',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='market', to='marketsdata.MarketType'),
        ),
    ]
