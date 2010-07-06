package org.garret.perst.impl;

import org.garret.perst.*;
import java.util.ArrayList;

public class RtreePage extends Persistent { 
    static class Branch extends Rectangle { 
        IPersistent p;

        Branch(Rectangle r, IPersistent p) { 
            super(r);
            this.p = p;
        }

        Branch() {}
    };
    static final int card = (Page.pageSize-ObjectHeader.sizeof-4-4)/(5*4);
    static final int minFill = card/2;

    int      n;
    Branch[] b;

    RtreePage(IPersistent obj, Rectangle r) {
        b = new Branch[card];
        n = 1;
        b[0] = new  Branch(r, obj);
        for (int i = 1; i < card; i++) { 
            b[i] = new Branch();
        }        
    }
    
    RtreePage(RtreePage root, RtreePage p) { 
        b = new Branch[card];
        n = 2;
        b[0] = new Branch(root.cover(), root);
        b[1] = new Branch(p.cover(), p);
        for (int i = 2; i < card; i++) { 
            b[i] = new Branch();
        }        
    }

    RtreePage() {}

    public boolean recursiveLoading() {
        return false;
    }

    RtreePage insert(Rectangle r, IPersistent obj, int level) {
        load();
        modify();
        if (--level != 0) { 
            // not leaf page
            int i, mini = 0;
            int minIncr = Integer.MAX_VALUE;
            int area = r.area();
            for (i = 0; i < n; i++) { 
                int incr = Rectangle.joinArea(b[i], r) - area;
                if (incr < minIncr) { 
                    minIncr = incr;
                    mini = i;
                }
            }
            RtreePage p = (RtreePage)b[mini].p;
            RtreePage q = p.insert(r, obj, level);
            if (q == null) { 
                // child was not split
                b[mini].join(r);
                return null;
            } else { 
                // child was split
                b[mini] = new Branch(p.cover(), p);
                return addBranch(new Branch(q.cover(), q));
            }
        } else { 
            return addBranch(new Branch(r, obj));
        }
    }

    int remove(Rectangle r, IPersistent obj, int level, ArrayList reinsertList) {
        load();
        modify();
        if (--level != 0) { 
            for (int i = 0; i < n; i++) { 
                if (r.intersects(b[i])) { 
                    RtreePage pg = (RtreePage)b[i].p;
                    int reinsertLevel = pg.remove(r, obj, level, reinsertList);
                    if (reinsertLevel >= 0) { 
                        if (pg.n >= minFill) { 
                            b[i] = new Branch(pg.cover(), pg);
                        } else { 
                            // not enough entries in child
                            reinsertList.add(pg);
                            reinsertLevel = level - 1;
                            removeBranch(i);
                        }
                        return reinsertLevel;
                    }
                }
            }
        } else {
            for (int i = 0; i < n; i++) { 
                if (b[i].p == obj) { 
                    removeBranch(i);
                    return 0;
                }
            }
        }
        return -1;        
    }

    void find(Rectangle r, ArrayList result, int level) {
        load();
        if (--level != 0) { /* this is an internal node in the tree */
            for (int i = 0; i < n; i++) { 
                if (r.intersects(b[i])) {
                    ((RtreePage)b[i].p).find(r, result, level); 
                }
            }
        } else { /* this is a leaf node */
            for (int i = 0; i < n; i++) { 
                if (r.intersects(b[i])) { 
                    IPersistent obj = b[i].p;
                    obj.load();
                    result.add(obj);
                }
            }
        }
    }

    void purge(int level) {
        load();
        if (--level != 0) { /* this is an internal node in the tree */
            for (int i = 0; i < n; i++) { 
                ((RtreePage)b[i].p).purge(level);
            }
        }
        deallocate();
    }
    
    final void removeBranch(int i) {
        n -= 1;
        System.arraycopy(b, i+1, b, i, n-i);
    }

    final RtreePage addBranch(Branch br) { 
        if (n < card) { 
            b[n++] = br;
            return null;
        } else { 
            return splitPage(br);
        }
    }

    final RtreePage splitPage(Branch br) { 
        int i, j, seed0 = 0, seed1 = 0;
        int[]  rectArea = new int[card+1];
        int    waste;
        int    worstWaste = Integer.MIN_VALUE;
        //
        // As the seeds for the two groups, find two rectangles which waste 
        // the most area if covered by a single rectangle.
        //
        rectArea[0] = br.area();
        for (i = 0; i < card; i++) { 
            rectArea[i+1] = b[i].area();
        }
        Branch bp = br;
        for (i = 0; i < card; i++) { 
            for (j = i+1; j <= card; j++) { 
                waste = Rectangle.joinArea(bp, b[j-1]) - rectArea[i] - rectArea[j];
                if (waste > worstWaste) {
                    worstWaste = waste;
                    seed0 = i;
                    seed1 = j;
                }
            }
            bp = b[i];
        }       
        byte[] taken = new byte[card];
        Rectangle group0, group1;
        int       groupArea0, groupArea1;
        int       groupCard0, groupCard1;
        RtreePage pg;

        taken[seed1-1] = 2;
        group1 = new Rectangle(b[seed1-1]);

        if (seed0 == 0) { 
            group0 = new Rectangle(br);
            pg = new RtreePage(br.p, br);
        } else { 
            group0 = new Rectangle(b[seed0-1]);
            pg = new RtreePage(b[seed0-1].p, group0);
            b[seed0-1] = br;
        }
        groupCard0 = groupCard1 = 1;
        groupArea0 = rectArea[seed0];
        groupArea1 = rectArea[seed1];
        //
        // Split remaining rectangles between two groups.
        // The one chosen is the one with the greatest difference in area 
        // expansion depending on which group - the rect most strongly 
        // attracted to one group and repelled from the other.
        //
        while (groupCard0 + groupCard1 < card + 1 
               && groupCard0 < card + 1 - minFill
               && groupCard1 < card + 1 - minFill)
        {
            int betterGroup = -1, chosen = -1;
            int biggestDiff = -1;
            for (i = 0; i < card; i++) { 
                if (taken[i] == 0) { 
                    int diff = (Rectangle.joinArea(group0, b[i]) - groupArea0)
                             - (Rectangle.joinArea(group1, b[i]) - groupArea1);
                    if (diff > biggestDiff || -diff > biggestDiff) { 
                        chosen = i;
                        if (diff < 0) { 
                            betterGroup = 0;
                            biggestDiff = -diff;
                        } else { 
                            betterGroup = 1;
                            biggestDiff = diff;
                        }
                    }
                }
            }
            Assert.that(chosen >= 0);
            if (betterGroup == 0) { 
                group0.join(b[chosen]);
                groupArea0 = group0.area();
                taken[chosen] = 1;
                pg.b[groupCard0++] = b[chosen];
            } else {
                groupCard1 += 1;
                group1.join(b[chosen]);
                groupArea1 = group1.area();
                taken[chosen] = 2;
            }
        }
        //
        // If one group gets too full, then remaining rectangle are
        // split between two groups in such way to balance cards of two groups.
        //
        if (groupCard0 + groupCard1 < card + 1) { 
            for (i = 0; i < card; i++) { 
                if (taken[i] == 0) { 
                    if (groupCard0 >= groupCard1) { 
                        taken[i] = 2;
                        groupCard1 += 1;
                    } else { 
                        taken[i] = 1;
                        pg.b[groupCard0++] = b[i];               
                    }
                }
            }
        }
        pg.n = groupCard0;
        n = groupCard1;
        for (i = 0, j = 0; i < groupCard1; j++) { 
            if (taken[j] == 2) {
                b[i++] = b[j];
            }
        }
        return pg;
    }   

    final Rectangle cover() {
        Rectangle r = new Rectangle(b[0]);
        for (int i = 1; i < n; i++) { 
            r.join(b[i]);
        }
        return r;
    }
}







