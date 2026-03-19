from django.db import models


class BaseTimeSeriesModel(models.Model):
    value = models.DecimalField(max_digits=12, decimal_places=2)
    date = models.DateField()
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class DollarQuote(BaseTimeSeriesModel):
    buy_value = models.DecimalField(max_digits=12, decimal_places=2)
    sell_value = models.DecimalField(max_digits=12, decimal_places=2)
    variation_daily = models.DecimalField(max_digits=8, decimal_places=2)
    source = models.CharField(max_length=120, default="mock")
    external_updated_at = models.DateTimeField()

    class Meta:
        verbose_name = "Cotizacion dolar blue"
        verbose_name_plural = "Cotizaciones dolar blue"
        ordering = ("-date", "-updated_at")

    def __str__(self) -> str:
        return f"Dolar blue {self.date} - venta {self.sell_value}"


class InflationData(BaseTimeSeriesModel):
    month_label = models.CharField(max_length=20)
    year_over_year = models.DecimalField(max_digits=8, decimal_places=2)

    class Meta:
        verbose_name = "Dato de inflacion"
        verbose_name_plural = "Datos de inflacion"
        ordering = ("date",)

    def __str__(self) -> str:
        return f"IPC {self.month_label}: {self.value}%"


class FuelPrice(BaseTimeSeriesModel):
    COMPANY_CHOICES = [
        ("YPF", "YPF"),
        ("Shell", "Shell"),
        ("Axion", "Axion"),
    ]
    FUEL_CHOICES = [
        ("super", "Nafta Super"),
        ("premium", "Nafta Premium"),
        ("gasoil", "Gasoil"),
    ]

    company = models.CharField(max_length=20, choices=COMPANY_CHOICES)
    province = models.CharField(max_length=80, default="BUENOS AIRES")
    fuel_type = models.CharField(max_length=20, choices=FUEL_CHOICES)

    class Meta:
        verbose_name = "Precio de combustible"
        verbose_name_plural = "Precios de combustibles"
        unique_together = ("company", "province", "fuel_type", "date")
        ordering = ("company", "province", "fuel_type", "-date")

    def __str__(self) -> str:
        return f"{self.company} {self.province} {self.get_fuel_type_display()} - {self.value}"


class AdrQuote(BaseTimeSeriesModel):
    company = models.CharField(max_length=120)
    local_ticker = models.CharField(max_length=20)
    usa_ticker = models.CharField(max_length=20)
    daily_change = models.DecimalField(max_digits=8, decimal_places=2)

    class Meta:
        verbose_name = "Cotizacion ADR"
        verbose_name_plural = "Cotizaciones ADR"
        unique_together = ("usa_ticker", "date")
        ordering = ("company",)

    def __str__(self) -> str:
        return f"{self.company} ({self.usa_ticker})"
