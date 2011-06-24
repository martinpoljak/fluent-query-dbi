# encoding: utf-8

module FluentQuery
    module Drivers
        class DBI

            ##
            # DBI prepared query.
            #

            class Prepared
            
                ##
                # Holds associated driver instance.
                #
                
                @driver
                
                ##
                # Holds appropriate query.
                #
                
                @query
                
                ##
                # Holds compiled query form.
                #
                
                @compiled
                
                ##
                # Holds prepared query form.
                #
                
                @prepared
                
                ##
                # Constructor.
                #
                
                def initialize(driver, query)
                    @driver = driver
                    @query = query
                end
                
                ##
                # Returns compiled query form.
                #
                
                def compiled
                    if @compiled.nil?
                        @compiled = @query.compile!
                    end
                    
                    @compiled
                end
                
                ##
                # Returns prepared query form.
                #
                
                def prepared
                    if @prepared.nil? or @callbacks.nil?
                        @prepared = ""
                        
                        self.compiled.raw.each do |token|
                            if token.kind_of? Proc
                                @prepared << @driver.quote_placeholder
                            else
                                @prepared << token.to_s
                            end
                        end
                    end
                    
                    @prepared
                end
                
                
                ##
                # Executes the query.
                #
                
                def execute
                end
                
            end
            
        end
    end
end

