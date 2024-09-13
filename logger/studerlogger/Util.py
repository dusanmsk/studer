from xcom_proto import XcomP as param




def xcomParamFromName(name):
    # try to parse name as a number
    paramId = None
    try:
        paramId = int(name)
    except:
        pass
    # if param name is number
    if paramId is not None:
        return param.getParamByID(paramId)
    # else use param name
    else:
        for datapoint in param._getDatapoints():
            if datapoint.name == name:
                return datapoint
    return None

def getStuderParameter(xcomProvider, parameterName, deviceAddress):
    try:
        param = xcomParamFromName(parameterName)
        if param:
            xcom = xcomProvider.get()
            if xcom:
                return xcom.getValue(param, int(deviceAddress))
    finally:
        xcomProvider.release()


def setStuderParameter(xcomProvider, parameterName, value, deviceAddress):
    try:
        param = xcomParamFromName(parameterName)
        if param:
            xcom = xcomProvider.get()
            if xcom:
                return xcom.setValue(param, value, int(deviceAddress))
    finally:
        xcomProvider.release()



class XcomMock:
    def getValue(self, param, deviceAddress):
        return 123.456

    def setValue(self, param, value, deviceAddress):
        print("MOCK Setting value", param, value, deviceAddress)

