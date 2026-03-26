# Copyright 2025 AstroLab Software
# Author: Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
from logging import Logger


def get_fink_logger(name: str = "test", log_level: str = "INFO") -> Logger:
    """Initialise python logger. Suitable for both driver and executors.

    Parameters
    ----------
    name : str
        Name of the application to be logged. Typically __name__ of a
        function or module.
    log_level : str
        Minimum level of log wanted: DEBUG, INFO, WARNING, ERROR, CRITICAL, OFF

    Returns
    -------
    logger : logging.Logger
        Python Logger

    Examples
    --------
    >>> log = get_fink_logger(__name__, "INFO")
    >>> log.info("Hi!")
    """
    # Format of the log message to be printed
    FORMAT = "%(asctime)-15s %(levelname)s %(message)s"
    DATEFORMAT = "%y/%m/%d %H:%M:%S"

    logger = logging.getLogger(name)

    # Only add handler if one doesn't already exist (prevents duplicates)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(FORMAT, datefmt=DATEFORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Set the minimum log level on the logger
    logger.setLevel(log_level)

    # Also set on handler to ensure it's not filtered there
    if logger.handlers:
        logger.handlers[0].setLevel(log_level)

    return logger
