package utils;

public class Queries {
    private static final String LAYOUT_QUERY_DIONE = """
            query q {
              machinesByType(machineType: "Schalung") {
                name
                currentWorkplaceGroup {
                  prodOrder {
                    orderNumber
                    workplaceGroups {
                      process {
                        name
                        processingTime
                        waitingTime
                      }
                      processStates {
                        startTime
                        endTime
                        isCompleted
                      }
                    }
                  }
                }
              }
            }
            """;
    private static final String MA_QUERY = """
            query ma {
              factoryWorkerByMaId(maId: MA_ID) {
                maId
                firstName
                lastName
              }
            }
            """;
    private static final String SCHALUNG_ID_QUERY_RHEA = """
            query q {
              prodOrderByMachineId(id: "MACHINE_ID") {
                workplaceGroups {
                  machines {
                    name
                  }
                }
              }
            }
            """;
    private static final String ALL_MACHINES_QUERY_RHEA = """
            query q {
              machinesByType(machineType: "Schalung") {
                name
                id
                currentWorkplaceGroup {
                  id
                }
              }
            }
            """;
    private static final String SCHALUNGS_SICHT_QUERY_RHEA = """
            query q {
              prodOrderByMachineId(id: "MACHINE_ID") {
                workplaceGroups {
                  id
                  process {
                    name
                  }
                  processStates {
                    startTime
                    endTime
                    isCompleted
                  }
                }
                orderPosition {
                  order {
                    project {
                      projectNumber
                    }
                  }
                  article {
                    name
                    boms {
                      bomName
                    }
                  }
                }
                productAssembly {
                  processList {
                    processes {
                      name
                    }
                  }
                }
              }
            }
            """;

    private Queries() {
    }

    public static String getLayoutQueryDione() {
        return LAYOUT_QUERY_DIONE;
    }

    public static String getMaQuery(String maId) {
        return MA_QUERY.replace("MA_ID", maId);
    }

    public static String getSchalungIdQueryRhea(String machineId) {
        return SCHALUNG_ID_QUERY_RHEA.replace("MACHINE_ID", machineId);
    }

    public static String getAllMachinesQueryRhea() {
        return ALL_MACHINES_QUERY_RHEA;
    }

    public static String getSchalungsSichtQueryRhea(String machineId) {
        return SCHALUNGS_SICHT_QUERY_RHEA.replace("MACHINE_ID", machineId);
    }
}
