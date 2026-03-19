from django.contrib import admin
from django.contrib.auth.views import LoginView, LogoutView
from django.urls import path

from dashboard.views import dashboard_view


urlpatterns = [
    path("admin/", admin.site.urls),
    path(
        "login/",
        LoginView.as_view(
            template_name="login.html",
            redirect_authenticated_user=True,
        ),
        name="login",
    ),
    path(
        "logout/",
        LogoutView.as_view(),
        name="logout",
    ),
    path("", dashboard_view, name="dashboard"),
]
