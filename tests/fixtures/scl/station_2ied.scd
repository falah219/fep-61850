<?xml version="1.0" encoding="UTF-8"?>
<!-- Fixture test SCL_IMPORT: SCD dua IED (IED2 memakai ldName). -->
<SCL xmlns="http://www.iec.ch/61850/2003/SCL" version="2007" revision="B">
  <Header id="gi-uji"/>
  <Communication>
    <SubNetwork name="StationBus" type="8-MMS">
      <ConnectedAP iedName="IED1" apName="AP1"><Address><P type="IP">10.0.0.1</P></Address></ConnectedAP>
      <ConnectedAP iedName="IED2" apName="AP1"><Address><P type="IP">10.0.0.2</P></Address></ConnectedAP>
    </SubNetwork>
  </Communication>
  <IED name="IED1" manufacturer="Vendor A" type="P1" configVersion="1">
    <AccessPoint name="AP1"><Server><LDevice inst="LD0">
      <LN0 lnClass="LLN0" inst="" lnType="LLN0_T"/>
      <LN lnClass="GGIO" inst="1" prefix="" lnType="GGIO_T"/>
    </LDevice></Server></AccessPoint>
  </IED>
  <IED name="IED2" manufacturer="Vendor B" type="P2" configVersion="2">
    <AccessPoint name="AP1"><Server><LDevice inst="LD0" ldName="BAY2_CTRL">
      <LN0 lnClass="LLN0" inst="" lnType="LLN0_T"/>
      <LN lnClass="GGIO" inst="2" prefix="" lnType="GGIO_T"/>
    </LDevice></Server></AccessPoint>
  </IED>
  <DataTypeTemplates>
    <LNodeType id="LLN0_T" lnClass="LLN0"/>
    <LNodeType id="GGIO_T" lnClass="GGIO"><DO name="Ind1" type="SPS_T"/></LNodeType>
    <DOType id="SPS_T" cdc="SPS"><DA name="stVal" fc="ST" bType="BOOLEAN"/><DA name="q" fc="ST" bType="Quality"/></DOType>
  </DataTypeTemplates>
</SCL>
