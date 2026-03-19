from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("dashboard", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="fuelprice",
            name="province",
            field=models.CharField(default="BUENOS AIRES", max_length=80),
        ),
        migrations.AlterModelOptions(
            name="fuelprice",
            options={
                "verbose_name": "Precio de combustible",
                "verbose_name_plural": "Precios de combustibles",
                "ordering": ("company", "province", "fuel_type", "-date"),
            },
        ),
        migrations.AlterUniqueTogether(
            name="fuelprice",
            unique_together={("company", "province", "fuel_type", "date")},
        ),
    ]
