from django.contrib.postgres.fields import ArrayField
from django.db import models

# Create your models here.
class Product(models.Model):
    id = models.IntegerField(primary_key=True)
    name = models.TextField()
    token = models.TextField()