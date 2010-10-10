package org.nachodb;

/**
 * Class representing relation between owner and members
 */
public abstract class Relation<M extends IPersistent, O extends IPersistent> extends Persistent implements Link<M> {
    /**
     * Get relation owner
     * @return owner of the relation
     */
    public O getOwner() { 
        return owner;
    }

    /**
     * Set relation owner
     * @param owner new owner of the relation
     */
    public void setOwner(O owner) { 
        this.owner = owner;
        modify();
    }
    
    /**
     * Relation constructor. Creates empty relation with specified owner and no members. 
     * Members can be added to the relation later.
     * @param owner owner of the relation
     */
    public Relation(O owner) {
        this.owner = owner;
    }
    
    protected Relation() {}

    private O owner;
}

