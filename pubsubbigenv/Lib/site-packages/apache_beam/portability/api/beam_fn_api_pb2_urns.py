from ..utils import PropertiesFromEnumValue
from . import metrics_pb2
EMPTY_MONITORING_INFO_LABEL_PROPS = metrics_pb2.MonitoringInfoLabelProps()
EMPTY_MONITORING_INFO_SPEC = metrics_pb2.MonitoringInfoSpec()

class LogEntry(object):

  class Severity(object):

    class Enum(object):
      UNSPECIFIED = PropertiesFromEnumValue(u'', u'', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
      TRACE = PropertiesFromEnumValue(u'', u'', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
      DEBUG = PropertiesFromEnumValue(u'', u'', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
      INFO = PropertiesFromEnumValue(u'', u'', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
      NOTICE = PropertiesFromEnumValue(u'', u'', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
      WARN = PropertiesFromEnumValue(u'', u'', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
      ERROR = PropertiesFromEnumValue(u'', u'', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
      CRITICAL = PropertiesFromEnumValue(u'', u'', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)

