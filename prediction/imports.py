import io
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pymongo
from pymongo.errors import PyMongoError
import pytz
from dotenv import load_dotenv
from sklearn.metrics import r2_score
from sklearn.preprocessing import StandardScaler

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader, Dataset