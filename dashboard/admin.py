from django.contrib import admin

from .models import AdrQuote, DollarQuote, FuelPrice, InflationData


@admin.register(DollarQuote)
class DollarQuoteAdmin(admin.ModelAdmin):
    list_display = ("date", "buy_value", "sell_value", "variation_daily", "updated_at")
    ordering = ("-date",)


@admin.register(InflationData)
class InflationDataAdmin(admin.ModelAdmin):
    list_display = ("month_label", "value", "year_over_year", "date", "updated_at")
    ordering = ("-date",)


@admin.register(FuelPrice)
class FuelPriceAdmin(admin.ModelAdmin):
    list_display = ("company", "province", "fuel_type", "value", "date", "updated_at")
    list_filter = ("company", "province", "fuel_type")
    ordering = ("company", "province", "fuel_type")


@admin.register(AdrQuote)
class AdrQuoteAdmin(admin.ModelAdmin):
    list_display = ("company", "local_ticker", "usa_ticker", "value", "daily_change", "date")
    ordering = ("company",)
