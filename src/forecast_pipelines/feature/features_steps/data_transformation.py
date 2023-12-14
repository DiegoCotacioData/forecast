from .methods.data_transformation_methods import PricesImputerOps, PricesSchemaOps


class TransformInputDataStep:

  def __init__(self, raw_sipsa_prices): #TODO: raw_abasto_volumes, raw_expo_volumnes

      self.raw_sipsa_prices = raw_sipsa_prices
      #self.raw_abasto_volumes = raw_abasto_volumes
      #self.raw_expo_volumnes = raw_expo_volumnes


  def transform_raw_data(self):
      
      """
      Initializes the TransformInputDataStep class.
   
      Args:
        raw_sipsa_prices: Raw SIPSA prices data.
        # raw_abasto_volumes: Raw abasto volumes data. (TODO: Uncomment and add if needed)
        # raw_expo_volumes: Raw expo volumes data. (TODO: Uncomment and add if needed)
      """

      sipsa_prices_transf = self.transform_prices_dataframe()
      #abasto_volumes_transf = self.transform_vol1_dataframe()
      #expo_volumes_transf = self.transform_vol1_dataframe()

      return sipsa_prices_transf #,abasto_volumes_transf , expo_volumes_transf 




  def transform_prices_dataframe(self):
      
      schema = PricesSchemaOps(self.raw_sipsa_prices)
      prices_transf =  schema.apply_schema_transformations()

      imputer = PricesImputerOps(prices_transf)
      sipsa_prices_transf = imputer.apply_imputer_transformations()
       
      return  sipsa_prices_transf


  #TODO:  Define transform methods for abasto_vols y expo_vols

  #def transform_abasto_volumes_dataframe(self):
      #pass

  #def transform_expo_volumes_dataframe(self):
      #pass