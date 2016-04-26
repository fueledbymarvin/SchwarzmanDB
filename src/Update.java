/**
 * Created by marvin on 4/25/16.
 */
public class Update {

    public enum Action {
        CREATE,
        DESTROY
    }
    private Action action;
    private Table table;
    private Projection projection;

    public Update(Action action, Table table, Projection projection) {

        this.action = action;
        this.table = table;
        this.projection = projection;
    }

    public Action getAction() {
        return action;
    }

    public Table getTable() {
        return table;
    }

    public Projection getProjection() {
        return projection;
    }
}
