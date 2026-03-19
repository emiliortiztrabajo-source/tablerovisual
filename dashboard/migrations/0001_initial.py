from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="AdrQuote",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("value", models.DecimalField(decimal_places=2, max_digits=12)),
                ("date", models.DateField()),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("company", models.CharField(max_length=120)),
                ("local_ticker", models.CharField(max_length=20)),
                ("usa_ticker", models.CharField(max_length=20)),
                ("daily_change", models.DecimalField(decimal_places=2, max_digits=8)),
            ],
            options={
                "verbose_name": "Cotizacion ADR",
                "verbose_name_plural": "Cotizaciones ADR",
                "ordering": ("company",),
                "unique_together": {("usa_ticker", "date")},
            },
        ),
        migrations.CreateModel(
            name="DollarQuote",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("value", models.DecimalField(decimal_places=2, max_digits=12)),
                ("date", models.DateField()),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("buy_value", models.DecimalField(decimal_places=2, max_digits=12)),
                ("sell_value", models.DecimalField(decimal_places=2, max_digits=12)),
                ("variation_daily", models.DecimalField(decimal_places=2, max_digits=8)),
                ("source", models.CharField(default="mock", max_length=120)),
                ("external_updated_at", models.DateTimeField()),
            ],
            options={
                "verbose_name": "Cotizacion dolar blue",
                "verbose_name_plural": "Cotizaciones dolar blue",
                "ordering": ("-date", "-updated_at"),
            },
        ),
        migrations.CreateModel(
            name="FuelPrice",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("value", models.DecimalField(decimal_places=2, max_digits=12)),
                ("date", models.DateField()),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("company", models.CharField(choices=[("YPF", "YPF"), ("Shell", "Shell"), ("Axion", "Axion")], max_length=20)),
                ("fuel_type", models.CharField(choices=[("super", "Nafta Super"), ("premium", "Nafta Premium"), ("gasoil", "Gasoil")], max_length=20)),
            ],
            options={
                "verbose_name": "Precio de combustible",
                "verbose_name_plural": "Precios de combustibles",
                "ordering": ("company", "fuel_type", "-date"),
                "unique_together": {("company", "fuel_type", "date")},
            },
        ),
        migrations.CreateModel(
            name="InflationData",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("value", models.DecimalField(decimal_places=2, max_digits=12)),
                ("date", models.DateField()),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("month_label", models.CharField(max_length=20)),
                ("year_over_year", models.DecimalField(decimal_places=2, max_digits=8)),
            ],
            options={
                "verbose_name": "Dato de inflacion",
                "verbose_name_plural": "Datos de inflacion",
                "ordering": ("date",),
            },
        ),
    ]
