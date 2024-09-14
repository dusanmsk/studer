






class XcomMock:
    def getValue(self, param, deviceAddress):
        return 123.456

    def setValue(self, param, value, deviceAddress):
        print("MOCK Setting value", param, value, deviceAddress)

