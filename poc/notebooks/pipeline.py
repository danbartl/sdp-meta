# Databricks notebook source
from pyspark.sql import SparkSession

from sdp_meta_poc.pipeline import initialize

initialize(SparkSession.active())
